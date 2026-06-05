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
        return _pool.get_nowait()
    except Exception:
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
        """)


def _migrate():
    """Додає нові колонки до існуючої БД (idempotent)."""
    migrations = [
        "ALTER TABLE leads ADD COLUMN last_rebroadcast_at REAL",
        "ALTER TABLE leads ADD COLUMN taken_at REAL",
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


def inc_taken(manager_id: str, month: str):
    q("""INSERT INTO stats (manager_id, month, taken) VALUES (?,?,1)
         ON CONFLICT(manager_id, month) DO UPDATE SET taken = taken + 1""",
      (manager_id, month))


def take_lead(lead_id: str, manager_id: str, month: str) -> bool:
    """Атомарно бере заявку і збільшує лічильник. Повертає True якщо взято цим менеджером."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "UPDATE leads SET status='taken', manager_id=?, taken_at=? "
            "WHERE lead_id=? AND status NOT IN ('taken','duplicate')",
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


def set_availability(manager_id: str, active: bool):
    q("""INSERT INTO availability (manager_id, is_active) VALUES (?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET is_active=?""",
      (manager_id, int(active), int(active)))


def mark_connected(manager_id: str, name: str):
    q("""INSERT INTO connected (manager_id, name, connected_at) VALUES (?,?,?)
         ON CONFLICT(manager_id) DO UPDATE SET name=?, connected_at=?""",
      (manager_id, name, datetime.now().timestamp(), name, datetime.now().timestamp()))


def get_connected() -> list:
    return [dict(r) for r in q("SELECT * FROM connected ORDER BY connected_at", fetch='all')]
