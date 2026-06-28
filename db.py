import logging
import sqlite3
from datetime import datetime
from queue import Full, Queue
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH    = 'leads.db'
_pool: Optional[Queue] = None
_POOL_SIZE = 5


def init_db():
    _init_pool()
    _create_tables()
    _migrate()


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _init_pool():
    global _pool
    _pool = Queue(maxsize=_POOL_SIZE)
    for _ in range(_POOL_SIZE):
        _pool.put(_make_conn())


def _get_conn() -> sqlite3.Connection:
    try:
        return _pool.get(timeout=5)
    except Exception:
        logger.warning("DB: пул з'єднань вичерпано, створюємо тимчасовий конект")
        return _make_conn()


def _release_conn(conn: sqlite3.Connection):
    try:
        _pool.put_nowait(conn)
    except Full:
        conn.close()


def _create_tables():
    with sqlite3.connect(DB_PATH) as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS leads (
                lead_id              TEXT PRIMARY KEY,
                status               TEXT DEFAULT 'queued',
                manager_id           TEXT,
                created_at           REAL NOT NULL,
                sent_at              REAL,
                taken_at             REAL,
                esc_level            INTEGER DEFAULT 0,
                title                TEXT,
                last_rebroadcast_at  REAL
            );
            CREATE TABLE IF NOT EXISTS messages (
                lead_id    TEXT NOT NULL,
                manager_id TEXT NOT NULL,
                msg_id     INTEGER NOT NULL,
                PRIMARY KEY (lead_id, manager_id)
            );
            CREATE TABLE IF NOT EXISTS stats (
                manager_id TEXT NOT NULL,
                month      TEXT NOT NULL,
                taken      INTEGER DEFAULT 0,
                PRIMARY KEY (manager_id, month)
            );
            CREATE TABLE IF NOT EXISTS skipped (
                lead_id    TEXT NOT NULL,
                manager_id TEXT NOT NULL,
                PRIMARY KEY (lead_id, manager_id)
            );
            CREATE TABLE IF NOT EXISTS availability (
                manager_id TEXT PRIMARY KEY,
                is_active  INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS connected (
                manager_id   TEXT PRIMARY KEY,
                name         TEXT,
                connected_at REAL
            );
            CREATE TABLE IF NOT EXISTS schedules (
                manager_id     TEXT PRIMARY KEY,
                days           TEXT NOT NULL DEFAULT '0,1,2,3,4',
                start_time     TEXT NOT NULL DEFAULT '16:00',
                enabled        INTEGER DEFAULT 1,
                last_notified  TEXT
            );
        """)


def _migrate():
    """Додає нові колонки до існуючої БД (idempotent)."""
    migrations = [
        "ALTER TABLE leads ADD COLUMN last_rebroadcast_at REAL",
        "ALTER TABLE leads ADD COLUMN taken_at REAL",
        "ALTER TABLE availability ADD COLUMN max_leads INTEGER",
        "ALTER TABLE availability ADD COLUMN exit_reason TEXT",
        "ALTER TABLE schedules ADD COLUMN end_time TEXT NOT NULL DEFAULT '23:00'",
    ]
    with sqlite3.connect(DB_PATH) as c:
        for sql in migrations:
            try:
                c.execute(sql)
            except sqlite3.OperationalError:
                pass


def q(sql: str, params=(), fetch: str = None):
    conn = _get_conn()
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        if fetch == 'one':
            return cur.fetchone()
        if fetch == 'all':
            return cur.fetchall()
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"DB помилка | SQL: {sql[:80]} | Params: {params} | Error: {e}")
        raise
    finally:
        _release_conn(conn)


def get_lead(lead_id: str):
    return q("SELECT * FROM leads WHERE lead_id=?", (lead_id,), fetch='one')


def get_taken(manager_id: str, month: str) -> int:
    row = q("SELECT taken FROM stats WHERE manager_id=? AND month=?",
            (manager_id, month), fetch='one')
    return int(row['taken']) if row else 0


def get_all_taken(month: str) -> dict:
    """Повертає {manager_id: taken} за місяць одним запитом."""
    rows = q("SELECT manager_id, taken FROM stats WHERE month=?", (month,), fetch='all')
    return {r['manager_id']: int(r['taken']) for r in rows} if rows else {}


def get_all_availability() -> dict:
    """Повертає {manager_id: bool} одним запитом."""
    rows = q("SELECT manager_id, is_active FROM availability", fetch='all')
    return {r['manager_id']: bool(r['is_active']) for r in rows} if rows else {}


def get_all_max_leads_overrides() -> dict:
    """Повертає {manager_id: max_leads} для менеджерів з ручним лімітом."""
    rows = q("SELECT manager_id, max_leads FROM availability WHERE max_leads IS NOT NULL", fetch='all')
    return {r['manager_id']: r['max_leads'] for r in rows} if rows else {}


def set_max_leads_override(manager_id: str, max_leads):
    """Встановлює або скидає ручний ліміт. max_leads=None → скинути (брати з таблиці)."""
    q("""INSERT INTO availability (manager_id, is_active, max_leads) VALUES (?, 1, ?)
         ON CONFLICT(manager_id) DO UPDATE SET max_leads=?""",
      (manager_id, max_leads, max_leads))


def reset_all_limit_overrides():
    """Скидає всі ручні ліміти о опівночі."""
    q("UPDATE availability SET max_leads=NULL WHERE max_leads IS NOT NULL")


def inc_taken(manager_id: str, month: str):
    q("""INSERT INTO stats (manager_id, month, taken) VALUES (?,?,1)
         ON CONFLICT(manager_id, month) DO UPDATE SET taken = taken + 1""",
      (manager_id, month))


def claim_lead_for_send(lead_id: str, manager_id: str) -> bool:
    """Атомарно «бронює» заявку для надсилання. Повертає True якщо заброньовано цим менеджером."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "UPDATE leads SET status='sent', manager_id=?, sent_at=? "
            "WHERE lead_id=? AND status IN ('queued','no_managers')",
            (manager_id, datetime.now().timestamp(), lead_id),
        )
        if cur.rowcount == 0:
            conn.rollback()
            return False
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"claim_lead_for_send | lead={lead_id} mgr={manager_id} | {e}")
        raise
    finally:
        _release_conn(conn)


def take_lead(lead_id: str, manager_id: str, month: str) -> bool:
    """Атомарно бере заявку і збільшує лічильник. Повертає True якщо взято цим менеджером."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "UPDATE leads SET status='taken', manager_id=?, taken_at=? "
            "WHERE lead_id=? AND status NOT IN ('taken','duplicate','closed')",
            (manager_id, datetime.now().timestamp(), lead_id),
        )
        if cur.rowcount == 0:
            conn.rollback()
            return False
        conn.execute(
            """INSERT INTO stats (manager_id, month, taken) VALUES (?,?,1)
               ON CONFLICT(manager_id, month) DO UPDATE SET taken = taken + 1""",
            (manager_id, month),
        )
        conn.commit()
        return True
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"take_lead | lead={lead_id} mgr={manager_id} | {e}")
        raise
    finally:
        _release_conn(conn)


def get_msg_id(lead_id: str, manager_id: str) -> Optional[int]:
    row = q("SELECT msg_id FROM messages WHERE lead_id=? AND manager_id=?",
            (lead_id, manager_id), fetch='one')
    return row['msg_id'] if row else None


def save_msg(lead_id: str, manager_id: str, msg_id: int):
    q("INSERT OR REPLACE INTO messages (lead_id, manager_id, msg_id) VALUES (?,?,?)",
      (lead_id, manager_id, msg_id))


def get_all_msgs(lead_id: str) -> list[dict]:
    rows = q("SELECT manager_id, msg_id FROM messages WHERE lead_id=?",
             (lead_id,), fetch='all')
    return [dict(r) for r in rows]


def mark_skipped(lead_id: str, manager_id: str):
    q("INSERT OR IGNORE INTO skipped (lead_id, manager_id) VALUES (?,?)",
      (lead_id, manager_id))


def get_skipped(lead_id: str) -> list[str]:
    rows = q("SELECT manager_id FROM skipped WHERE lead_id=?", (lead_id,), fetch='all')
    return [r['manager_id'] for r in rows]


def is_available(manager_id: str) -> bool:
    row = q("SELECT is_active FROM availability WHERE manager_id=?",
            (manager_id,), fetch='one')
    return bool(row['is_active']) if row else False


def get_all_exit_reasons() -> dict:
    """Повертає {manager_id: exit_reason} для менеджерів поза чергою."""
    rows = q("SELECT manager_id, exit_reason FROM availability WHERE is_active=0 AND exit_reason IS NOT NULL",
             fetch='all')
    return {r['manager_id']: r['exit_reason'] for r in rows} if rows else {}


def set_availability(manager_id: str, active: bool, reason: str = None):
    exit_reason = None if active else (reason or 'manual')
    q("""INSERT INTO availability (manager_id, is_active, exit_reason) VALUES (?, ?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET is_active=?, exit_reason=?""",
      (manager_id, int(active), exit_reason, int(active), exit_reason))


def mark_connected(manager_id: str, name: str):
    ts = datetime.now().timestamp()
    q("""INSERT INTO connected (manager_id, name, connected_at) VALUES (?,?,?)
         ON CONFLICT(manager_id) DO UPDATE SET name=?, connected_at=?""",
      (manager_id, name, ts, name, ts))


def get_connected() -> list:
    return [dict(r) for r in q("SELECT * FROM connected ORDER BY connected_at", fetch='all')]


# ─── РОЗКЛАДИ ────────────────────────────────────────────────────────────────

def get_schedule(manager_id: str) -> Optional[dict]:
    row = q("SELECT * FROM schedules WHERE manager_id=?", (manager_id,), fetch='one')
    return dict(row) if row else None


def get_all_schedules() -> dict:
    """Повертає {manager_id: {days, start_time, enabled, last_notified}}."""
    rows = q("SELECT * FROM schedules", fetch='all')
    return {r['manager_id']: dict(r) for r in rows} if rows else {}


def set_schedule(manager_id: str, days: str, start_time: str, end_time: str):
    """days — рядок '0,1,2,3,4' (пн=0, нд=6)."""
    q("""INSERT INTO schedules (manager_id, days, start_time, end_time) VALUES (?, ?, ?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET days=?, start_time=?, end_time=?""",
      (manager_id, days, start_time, end_time, days, start_time, end_time))


def set_schedule_enabled(manager_id: str, enabled: bool):
    q("""INSERT INTO schedules (manager_id, enabled) VALUES (?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET enabled=?""",
      (manager_id, int(enabled), int(enabled)))


def update_last_notified(manager_id: str, date_str: str):
    """date_str формат 'YYYY-MM-DD'."""
    q("UPDATE schedules SET last_notified=? WHERE manager_id=?", (date_str, manager_id))


def init_default_schedules(managers_map: dict):
    """
    Заповнює таблицю розкладів дефолтними значеннями якщо вони ще не задані.
    managers_map: {name: tg_id}
    """
    DEFAULTS = {
        '7083918297': ('0,1,2,3,6', '22:00', '05:00'),  # Льоша: пн-чт + нд, 22:00–05:00
        '8762578305': ('0,1,2,4,5', '16:00', '23:00'),  # Федя:  пн-ср + пт-сб, 16:00–23:00
    }
    DEFAULT_DAYS      = '0,1,2,3,4'
    DEFAULT_START     = '16:00'
    DEFAULT_END       = '23:00'

    existing = set(r['manager_id'] for r in (q("SELECT manager_id FROM schedules", fetch='all') or []))

    with sqlite3.connect(DB_PATH) as c:
        for name, tg_id in managers_map.items():
            if tg_id in existing:
                continue
            days, start_time, end_time = DEFAULTS.get(tg_id, (DEFAULT_DAYS, DEFAULT_START, DEFAULT_END))
            c.execute(
                "INSERT OR IGNORE INTO schedules (manager_id, days, start_time, end_time) VALUES (?,?,?,?)",
                (tg_id, days, start_time, end_time),
            )
