import logging
from datetime import datetime
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = 'leads.db'

# ─── Єдине спільне з'єднання ─────────────────────────────────────────────────
# aiosqlite.Connection сам по собі виконує операції у фоновому потоці, тож
# окремий пул із кількох з'єднань не додає паралелізму на запис (SQLite
# все одно дозволяє лише одного писача одночасно) — натомість ускладнює
# атомарність ручних транзакцій (claim_lead_for_send/take_lead). Тому тут
# одне спільне з'єднання + asyncio.Lock(), який серіалізує кожну логічну
# операцію (execute[...]+commit/rollback) від початку до кінця — це усуває
# ризик того, що дві паралельні "транзакції" на одному з'єднанні переплетуться.
_conn: Optional[aiosqlite.Connection] = None
_conn_lock = None  # asyncio.Lock() — створюється лениво в init_db(), бо потребує запущеного event loop


async def init_db():
    global _conn, _conn_lock
    import asyncio
    _conn_lock = asyncio.Lock()

    _conn = await aiosqlite.connect(DB_PATH)
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute("PRAGMA busy_timeout=5000")
    await _create_tables()
    await _migrate()


async def _create_tables():
    await _conn.executescript("""
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
            active     INTEGER NOT NULL DEFAULT 1,
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
        CREATE TABLE IF NOT EXISTS managers (
            tg_id       TEXT PRIMARY KEY,
            tg_name     TEXT,
            sheet_name  TEXT,
            kommo_id    INTEGER,
            is_approved INTEGER DEFAULT 0,
            created_at  REAL
        );
        CREATE TABLE IF NOT EXISTS distributed_leads (
            lead_id    TEXT PRIMARY KEY,
            manager_id TEXT NOT NULL,
            kept_in_queue INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS status_chats (
            chat_id    TEXT PRIMARY KEY,
            added_at   REAL
        );
        CREATE TABLE IF NOT EXISTS held_leads (
            lead_id    TEXT PRIMARY KEY,
            title      TEXT,
            phone      TEXT,
            timezone   TEXT NOT NULL,
            created_at REAL NOT NULL
        );
    """)
    await _conn.commit()


async def _migrate():
    """Додає нові колонки до існуючої БД (idempotent)."""
    migrations = [
        "ALTER TABLE leads ADD COLUMN last_rebroadcast_at REAL",
        "ALTER TABLE leads ADD COLUMN taken_at REAL",
        "ALTER TABLE availability ADD COLUMN max_leads INTEGER",
        "ALTER TABLE availability ADD COLUMN exit_reason TEXT",
        "ALTER TABLE schedules ADD COLUMN end_time TEXT NOT NULL DEFAULT '23:00'",
        "ALTER TABLE messages ADD COLUMN active INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE distributed_leads ADD COLUMN kept_in_queue INTEGER NOT NULL DEFAULT 0",
    ]
    for sql in migrations:
        try:
            await _conn.execute(sql)
        except aiosqlite.OperationalError:
            pass
    await _conn.commit()


async def q(sql: str, params=(), fetch: str = None):
    async with _conn_lock:
        try:
            cur = await _conn.execute(sql, params)
            await _conn.commit()
            if fetch == 'one':
                return await cur.fetchone()
            if fetch == 'all':
                return await cur.fetchall()
        except Exception as e:
            await _conn.rollback()
            logger.error(f"DB помилка | SQL: {sql[:80]} | Params: {params} | Error: {e}")
            raise


async def get_lead(lead_id: str):
    return await q("SELECT * FROM leads WHERE lead_id=?", (lead_id,), fetch='one')


async def get_taken(manager_id: str, month: str) -> int:
    row = await q("SELECT taken FROM stats WHERE manager_id=? AND month=?",
                  (manager_id, month), fetch='one')
    return int(row['taken']) if row else 0


async def get_all_taken(month: str) -> dict:
    """Повертає {manager_id: taken} за місяць одним запитом."""
    rows = await q("SELECT manager_id, taken FROM stats WHERE month=?", (month,), fetch='all')
    return {r['manager_id']: int(r['taken']) for r in rows} if rows else {}


async def get_all_availability() -> dict:
    """Повертає {manager_id: bool} одним запитом."""
    rows = await q("SELECT manager_id, is_active FROM availability", fetch='all')
    return {r['manager_id']: bool(r['is_active']) for r in rows} if rows else {}


async def get_all_max_leads_overrides() -> dict:
    """Повертає {manager_id: max_leads} для менеджерів з ручним лімітом."""
    rows = await q("SELECT manager_id, max_leads FROM availability WHERE max_leads IS NOT NULL", fetch='all')
    return {r['manager_id']: r['max_leads'] for r in rows} if rows else {}


async def set_max_leads_override(manager_id: str, max_leads):
    """Встановлює або скидає ручний ліміт. max_leads=None → скинути (брати з таблиці)."""
    await q("""INSERT INTO availability (manager_id, is_active, max_leads) VALUES (?, 1, ?)
         ON CONFLICT(manager_id) DO UPDATE SET max_leads=?""",
      (manager_id, max_leads, max_leads))


async def reset_all_limit_overrides():
    """Скидає всі ручні ліміти о опівночі."""
    await q("UPDATE availability SET max_leads=NULL WHERE max_leads IS NOT NULL")


async def inc_taken(manager_id: str, month: str):
    await q("""INSERT INTO stats (manager_id, month, taken) VALUES (?,?,1)
         ON CONFLICT(manager_id, month) DO UPDATE SET taken = taken + 1""",
      (manager_id, month))


async def transfer_taken(old_manager_id: str, new_manager_id: Optional[str], day: str):
    """
    Переносить облік «взятої» заявки з одного менеджера на іншого — коли
    в Kommo змінили відповідального, поки лід лишався в 'Распределены'.
    Зменшує лічильник старому (не нижче 0); якщо новий відповідальний теж
    наш менеджер — збільшує йому, ніби це він щойно взяв заявку.
    """
    await q("UPDATE stats SET taken = MAX(taken - 1, 0) WHERE manager_id=? AND month=?",
      (old_manager_id, day))
    if new_manager_id:
        await q("""INSERT INTO stats (manager_id, month, taken) VALUES (?,?,1)
             ON CONFLICT(manager_id, month) DO UPDATE SET taken = taken + 1""",
          (new_manager_id, day))


async def claim_lead_for_send(lead_id: str, manager_id: str) -> bool:
    """Атомарно «бронює» заявку для надсилання. Повертає True якщо заброньовано цим менеджером."""
    async with _conn_lock:
        try:
            cur = await _conn.execute(
                "UPDATE leads SET status='sent', manager_id=?, sent_at=? "
                "WHERE lead_id=? AND status IN ('queued','no_managers')",
                (manager_id, datetime.now().timestamp(), lead_id),
            )
            if cur.rowcount == 0:
                await _conn.rollback()
                return False
            await _conn.commit()
            return True
        except Exception as e:
            await _conn.rollback()
            logger.error(f"claim_lead_for_send | lead={lead_id} mgr={manager_id} | {e}")
            raise


async def take_lead(lead_id: str, manager_id: str, month: str) -> bool:
    """Атомарно бере заявку і збільшує лічильник. Повертає True якщо взято цим менеджером."""
    async with _conn_lock:
        try:
            cur = await _conn.execute(
                "UPDATE leads SET status='taken', manager_id=?, taken_at=? "
                "WHERE lead_id=? AND status NOT IN ('taken','duplicate','closed')",
                (manager_id, datetime.now().timestamp(), lead_id),
            )
            if cur.rowcount == 0:
                await _conn.rollback()
                return False
            await _conn.execute(
                """INSERT INTO stats (manager_id, month, taken) VALUES (?,?,1)
                   ON CONFLICT(manager_id, month) DO UPDATE SET taken = taken + 1""",
                (manager_id, month),
            )
            await _conn.commit()
            return True
        except Exception as e:
            await _conn.rollback()
            logger.error(f"take_lead | lead={lead_id} mgr={manager_id} | {e}")
            raise


async def add_status_chat(chat_id: str):
    await q("INSERT OR REPLACE INTO status_chats (chat_id, added_at) VALUES (?, ?)",
      (str(chat_id), datetime.now().timestamp()))


async def remove_status_chat(chat_id: str):
    await q("DELETE FROM status_chats WHERE chat_id=?", (str(chat_id),))


async def get_status_chats() -> list:
    rows = await q("SELECT chat_id FROM status_chats", fetch='all')
    return [r['chat_id'] for r in rows] if rows else []


async def get_msg_id(lead_id: str, manager_id: str) -> Optional[int]:
    row = await q("SELECT msg_id FROM messages WHERE lead_id=? AND manager_id=?",
                  (lead_id, manager_id), fetch='one')
    return row['msg_id'] if row else None


async def save_msg(lead_id: str, manager_id: str, msg_id: int):
    await q("INSERT OR REPLACE INTO messages (lead_id, manager_id, msg_id, active) VALUES (?,?,?,1)",
      (lead_id, manager_id, msg_id))


async def set_msg_active(lead_id: str, manager_id: str, active: bool):
    """Позначає, чи повідомлення досі 'живе' (з кнопкою) — для коректної діагностики."""
    await q("UPDATE messages SET active=? WHERE lead_id=? AND manager_id=?",
      (1 if active else 0, lead_id, manager_id))


async def delete_msg(lead_id: str, manager_id: str):
    await q("DELETE FROM messages WHERE lead_id=? AND manager_id=?", (lead_id, manager_id))


async def get_all_msgs(lead_id: str) -> list[dict]:
    rows = await q("SELECT manager_id, msg_id FROM messages WHERE lead_id=?",
                   (lead_id,), fetch='all')
    return [dict(r) for r in rows]


async def mark_skipped(lead_id: str, manager_id: str):
    await q("INSERT OR IGNORE INTO skipped (lead_id, manager_id) VALUES (?,?)",
      (lead_id, manager_id))


async def get_skipped(lead_id: str) -> list[str]:
    rows = await q("SELECT manager_id FROM skipped WHERE lead_id=?", (lead_id,), fetch='all')
    return [r['manager_id'] for r in rows]


async def is_available(manager_id: str) -> bool:
    row = await q("SELECT is_active FROM availability WHERE manager_id=?",
                  (manager_id,), fetch='one')
    return bool(row['is_active']) if row else False


async def get_exit_reason(manager_id: str) -> Optional[str]:
    """Повертає поточний exit_reason менеджера (None якщо в черзі або запису нема)."""
    row = await q("SELECT exit_reason FROM availability WHERE manager_id=?",
                  (manager_id,), fetch='one')
    return row['exit_reason'] if row else None


async def get_all_exit_reasons() -> dict:
    """Повертає {manager_id: exit_reason} для менеджерів поза чергою."""
    rows = await q("SELECT manager_id, exit_reason FROM availability WHERE is_active=0 AND exit_reason IS NOT NULL",
                   fetch='all')
    return {r['manager_id']: r['exit_reason'] for r in rows} if rows else {}


async def set_availability(manager_id: str, active: bool, reason: str = None):
    exit_reason = None if active else (reason or 'manual')
    await q("""INSERT INTO availability (manager_id, is_active, exit_reason) VALUES (?, ?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET is_active=?, exit_reason=?""",
      (manager_id, int(active), exit_reason, int(active), exit_reason))


async def get_last_connected_ts(manager_id: str) -> float:
    """Повертає timestamp останнього підключення або 0.0 якщо ніколи."""
    row = await q("SELECT connected_at FROM connected WHERE manager_id=?", (manager_id,), fetch='one')
    return float(row['connected_at']) if row else 0.0


async def mark_connected(manager_id: str, name: str):
    ts = datetime.now().timestamp()
    await q("""INSERT INTO connected (manager_id, name, connected_at) VALUES (?,?,?)
         ON CONFLICT(manager_id) DO UPDATE SET name=?, connected_at=?""",
      (manager_id, name, ts, name, ts))


async def get_connected() -> list:
    rows = await q("SELECT * FROM connected ORDER BY connected_at", fetch='all')
    return [dict(r) for r in rows]


# ─── РОЗКЛАДИ ────────────────────────────────────────────────────────────────

async def get_schedule(manager_id: str) -> Optional[dict]:
    row = await q("SELECT * FROM schedules WHERE manager_id=?", (manager_id,), fetch='one')
    return dict(row) if row else None


async def get_all_schedules() -> dict:
    """Повертає {manager_id: {days, start_time, enabled, last_notified}}."""
    rows = await q("SELECT * FROM schedules", fetch='all')
    return {r['manager_id']: dict(r) for r in rows} if rows else {}


async def set_schedule(manager_id: str, days: str, start_time: str, end_time: str):
    """days — рядок '0,1,2,3,4' (пн=0, нд=6)."""
    await q("""INSERT INTO schedules (manager_id, days, start_time, end_time) VALUES (?, ?, ?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET days=?, start_time=?, end_time=?""",
      (manager_id, days, start_time, end_time, days, start_time, end_time))


async def set_schedule_enabled(manager_id: str, enabled: bool):
    await q("""INSERT INTO schedules (manager_id, enabled) VALUES (?, ?)
         ON CONFLICT(manager_id) DO UPDATE SET enabled=?""",
      (manager_id, int(enabled), int(enabled)))


async def update_last_notified(manager_id: str, date_str: str):
    """date_str формат 'YYYY-MM-DD'."""
    await q("UPDATE schedules SET last_notified=? WHERE manager_id=?", (date_str, manager_id))


async def init_default_schedules(managers_map: dict):
    """
    Заповнює таблицю розкладів дефолтними значеннями якщо вони ще не задані.
    managers_map: {name: tg_id}
    """
    DEFAULT_DAYS      = '0,1,2,3,4'
    DEFAULT_START     = '16:00'
    DEFAULT_END       = '23:00'

    rows = await q("SELECT manager_id FROM schedules", fetch='all')
    existing = set(r['manager_id'] for r in (rows or []))

    async with _conn_lock:
        for name, tg_id in managers_map.items():
            if tg_id in existing:
                continue
            await _conn.execute(
                "INSERT OR IGNORE INTO schedules (manager_id, days, start_time, end_time) VALUES (?,?,?,?)",
                (tg_id, DEFAULT_DAYS, DEFAULT_START, DEFAULT_END),
            )
        await _conn.commit()


# ─── МЕНЕДЖЕРИ ───────────────────────────────────────────────────────────────

async def get_manager(tg_id: str) -> Optional[dict]:
    row = await q("SELECT * FROM managers WHERE tg_id=?", (tg_id,), fetch='one')
    return dict(row) if row else None


async def get_manager_by_sheet_name(sheet_name: str) -> Optional[str]:
    """Повертає tg_id менеджера за іменем у Google Sheets, або None."""
    row = await q("SELECT tg_id FROM managers WHERE sheet_name=? AND is_approved=1",
                  (sheet_name,), fetch='one')
    return row['tg_id'] if row else None


async def get_all_managers(approved_only: bool = False) -> list:
    if approved_only:
        rows = await q("SELECT * FROM managers WHERE is_approved=1 ORDER BY sheet_name", fetch='all')
    else:
        rows = await q("SELECT * FROM managers ORDER BY created_at", fetch='all')
    return [dict(r) for r in rows] if rows else []


async def get_managers_dict() -> dict:
    """Повертає {sheet_name: tg_id} для всіх схвалених менеджерів."""
    rows = await get_all_managers(approved_only=True)
    return {r['sheet_name']: r['tg_id'] for r in rows if r['sheet_name']}


async def get_pending_managers() -> list:
    rows = await q("SELECT * FROM managers WHERE is_approved=0 ORDER BY created_at", fetch='all')
    return [dict(r) for r in rows] if rows else []


async def upsert_manager(tg_id: str, tg_name: str, sheet_name: str = None, kommo_id: int = None):
    """Створює або оновлює запис менеджера (не схвалений)."""
    ts = datetime.now().timestamp()
    await q("""INSERT INTO managers (tg_id, tg_name, sheet_name, kommo_id, is_approved, created_at)
         VALUES (?, ?, ?, ?, 0, ?)
         ON CONFLICT(tg_id) DO UPDATE SET tg_name=?, sheet_name=?, kommo_id=?""",
      (tg_id, tg_name, sheet_name, kommo_id, ts, tg_name, sheet_name, kommo_id))


async def approve_manager(tg_id: str):
    await q("UPDATE managers SET is_approved=1 WHERE tg_id=?", (tg_id,))


async def delete_manager(tg_id: str):
    """
    Видаляє менеджера і весь пов'язаний РАНТАЙМ-стан (не історію).

    Раніше чистилась тільки таблиця managers, а schedules/availability/connected
    лишались "сирітськими" записами назавжди. Найгірший наслідок — schedules:
    _check_schedules() в queue_logic.py читає ЦІЛУ таблицю schedules напряму
    (SELECT * FROM schedules, без JOIN на managers) і щодня на початку зміни
    намагається надіслати нагадування навіть видаленому/заблокованому менеджеру.
    Той знову кидає Forbidden → знову _deactivate_blocked() → знову сповіщення
    адмінам "заблокував бота" — щодня, нескінченно, для вже видаленого юзера.

    leads/messages/stats/skipped/distributed_leads навмисно НЕ чіпаємо —
    це історія (статистика, лог заявок), вона має лишитись.
    """
    await q("DELETE FROM managers WHERE tg_id=?", (tg_id,))
    await q("DELETE FROM schedules WHERE manager_id=?", (tg_id,))
    await q("DELETE FROM availability WHERE manager_id=?", (tg_id,))
    await q("DELETE FROM connected WHERE manager_id=?", (tg_id,))


# ─── РОЗПОДІЛЕНІ ЗАЯВКИ ──────────────────────────────────────────────────────

async def add_distributed_lead(lead_id: str, manager_id: str, kept_in_queue: bool = False):
    """
    Фіксує що заявка перейшла в 'Распределены' і прив'язана до менеджера.

    kept_in_queue=True — особливий випадок (напр. джерело "Реактивация"):
    заявку рахуємо як завжди, але менеджера з черги НЕ виводили, тому
    on_lead_undistributed пізніше не повинен повертати його в чергу /
    слати "вас повернуто в чергу" — він з неї й не виходив.
    """
    await q("INSERT OR REPLACE INTO distributed_leads (lead_id, manager_id, kept_in_queue) VALUES (?,?,?)",
      (lead_id, manager_id, int(kept_in_queue)))


async def remove_distributed_lead(lead_id: str):
    """Видаляє запис коли заявка покинула статус 'Распределены'."""
    await q("DELETE FROM distributed_leads WHERE lead_id=?", (lead_id,))


async def get_distributed_lead(lead_id: str) -> Optional[dict]:
    """Повертає {lead_id, manager_id} якщо заявка зараз у 'Распределены', інакше None."""
    row = await q("SELECT * FROM distributed_leads WHERE lead_id=?", (lead_id,), fetch='one')
    return dict(row) if row else None


async def count_distributed_leads(manager_id: str) -> int:
    """Кількість активних 'Распределены' заявок у менеджера."""
    row = await q("SELECT COUNT(*) as cnt FROM distributed_leads WHERE manager_id=?",
                  (manager_id,), fetch='one')
    return int(row['cnt']) if row else 0


async def get_manager_distributed_leads(manager_id: str) -> list[str]:
    """Список lead_id усіх заявок, які зараз у 'Распределены' на цьому менеджері."""
    rows = await q("SELECT lead_id FROM distributed_leads WHERE manager_id=?",
                   (manager_id,), fetch='all')
    return [r['lead_id'] for r in (rows or [])]


# ─── ЗАЯВКИ НА УТРИМАННІ (до 9:00 за часом клієнта) ─────────────────────────

async def add_held_lead(lead_id: str, title: str, phone: Optional[str], timezone: str):
    """
    Фіксує заявку, яку Kommo вже прислав, але клієнту в його часовому поясі
    ще не настало 9:00 — тому менеджеру її поки не показуємо (див. webhook.py).
    """
    await q("""INSERT OR REPLACE INTO held_leads (lead_id, title, phone, timezone, created_at)
         VALUES (?,?,?,?,?)""",
      (lead_id, title, phone, timezone, datetime.now().timestamp()))


async def get_held_lead(lead_id: str) -> Optional[dict]:
    row = await q("SELECT * FROM held_leads WHERE lead_id=?", (lead_id,), fetch='one')
    return dict(row) if row else None


async def remove_held_lead(lead_id: str):
    await q("DELETE FROM held_leads WHERE lead_id=?", (lead_id,))


async def get_all_held_leads() -> list[dict]:
    rows = await q("SELECT * FROM held_leads", fetch='all')
    return [dict(r) for r in rows] if rows else []


async def migrate_managers_from_config(managers_dict: dict, kommo_ids_dict: dict):
    """
    Переносить хардкоджених менеджерів з config.py у таблицю managers.
    Ідемпотентна — пропускає вже існуючих.
    """
    ts = datetime.now().timestamp()
    count = 0
    async with _conn_lock:
        for sheet_name, tg_id in managers_dict.items():
            cur = await _conn.execute(
                """INSERT OR IGNORE INTO managers
                   (tg_id, tg_name, sheet_name, kommo_id, is_approved, created_at)
                   VALUES (?, ?, ?, ?, 1, ?)""",
                (tg_id, sheet_name, sheet_name, kommo_ids_dict.get(tg_id), ts),
            )
            if cur.rowcount:
                count += 1
        await _conn.commit()
    if count:
        logger.info(f"migrate_managers_from_config: додано {count} менеджерів")


async def close_db():
    """Закриває спільне з'єднання (виклик при graceful shutdown)."""
    global _conn
    if _conn is not None:
        await _conn.close()
        _conn = None
