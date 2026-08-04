import json
import sqlite3
import functools
import pandas as pd
from pathlib import Path
from contextlib import contextmanager

from queries import get_anomalies_query, get_insert_anomaly_query, get_close_anomaly_query, get_cancel_anomaly_query

# --- НАСТРОЙКИ ПУТЕЙ ---
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.json"

def load_config() -> dict:
    """Загружает конфигурацию из config.json"""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

DB_PATH = BASE_DIR / CONFIG['paths']['data_dir'] / CONFIG['paths']['db_name']

@contextmanager
def get_connection(): 
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

@functools.lru_cache(maxsize=1)
def get_db_stats():
    if not DB_PATH.exists(): return None
    with get_connection() as conn:
        res = conn.execute("SELECT MIN(SUBSTR(report_timestamp, 1, 10)), MAX(SUBSTR(report_timestamp, 1, 10)), COUNT(DISTINCT SUBSTR(report_timestamp, 1, 10)) FROM stocks").fetchone()
        return {"start": res[0], "end": res[1], "days_count": res[2]}

def update_anomaly_inbox():
    """
    Фоновый сборщик аномалий.

    Жизненный цикл записи в inbox:
      1. Парсер обнаружил скачок остатка → INSERT (если товара нет в inbox)
         или UPDATE (если товар уже ждёт проверки — обновляем дельту).
      2. Запись висит в inbox бессрочно, пока пользователь не обработает её.
      3. При обработке — DELETE из inbox (функция save_anomaly_to_db).

    Один товар = максимум одна строка в inbox (UNIQUE по item_name).
    """
    if not DB_PATH.exists(): return
    with get_connection() as conn:
        # ── Миграция: если таблица со старой схемой UNIQUE(detected_date, item_name),
        #    пересоздаём с UNIQUE(item_name) и дедуплицируем данные. ─────────────
        _migrate_inbox_schema(conn)

        # FIX 4: гарантируем существование item_aliases до запроса аномалий
        conn.execute("""
            CREATE TABLE IF NOT EXISTS item_aliases (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                new_name TEXT NOT NULL,
                old_name TEXT NOT NULL,
                UNIQUE (new_name, old_name)
            )
        """)
        conn.commit()

        cursor = conn.execute("SELECT DISTINCT SUBSTR(report_timestamp, 1, 10) FROM stocks ORDER BY 1 DESC LIMIT 2")
        dates = [row[0] for row in cursor.fetchall()]
        if len(dates) < 2: return
        
        today, yesterday = dates[0], dates[1]
        
        query = get_anomalies_query()
        df = pd.read_sql_query(query, conn, params={"yesterday": yesterday, "today": today})
        
        for _, row in df.iterrows():
            # Upsert: INSERT новый товар, или UPDATE дельту если он уже ждёт проверки.
            # При UPDATE сохраняем оригинальную detected_date (дата первого обнаружения),
            # но обновляем qty_old, qty_new и delta — актуальные цифры для проверки.
            conn.execute("""
                INSERT INTO anomaly_inbox
                (detected_date, sku, item_name, qty_old, qty_new, delta,
                 history_count, old_name_alias, old_sku_alias)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_name) DO UPDATE SET
                    qty_old = excluded.qty_old,
                    qty_new = excluded.qty_new,
                    delta   = excluded.delta,
                    sku     = excluded.sku
            """, (today, row['sku'], row['item_name'], row['qty_old'], row['qty_new'],
                  row['delta'], row['history_count'], row['old_name_alias'], row['old_sku_alias']))
        conn.commit()


def _migrate_inbox_schema(conn):
    """
    Однократная миграция: старая схема UNIQUE(detected_date, item_name)
    → новая схема UNIQUE(item_name).
    Дедуплицирует данные (оставляет самую свежую запись на каждый товар).
    """
    # Проверяем текущую схему
    index_info = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='anomaly_inbox'"
    ).fetchone()

    if index_info is None:
        # Таблицы нет — создаём сразу с правильной схемой
        conn.execute("""
            CREATE TABLE anomaly_inbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_date DATE,
                sku TEXT,
                item_name TEXT UNIQUE,
                qty_old INTEGER,
                qty_new INTEGER,
                delta INTEGER,
                history_count INTEGER,
                old_name_alias TEXT,
                old_sku_alias TEXT
            )
        """)
        conn.commit()
        return

    schema_sql = index_info[0] or ''
    if 'UNIQUE' not in schema_sql or 'detected_date' not in schema_sql:
        # Схема уже новая (или нет UNIQUE вообще) — пропускаем
        # Но на всякий случай чистим старые индексы
        conn.execute("DROP INDEX IF EXISTS idx_inbox_active_item")
        return

    # Старая схема — мигрируем
    import logging
    _log = logging.getLogger('shadow_stock')

    # Чистим от предыдущей прерванной миграции (если была)
    conn.execute("DROP TABLE IF EXISTS anomaly_inbox_new")

    # 1. Сохраняем только самую свежую запись на каждый item_name
    conn.execute("""
        CREATE TABLE anomaly_inbox_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            detected_date DATE,
            sku TEXT,
            item_name TEXT UNIQUE,
            qty_old INTEGER,
            qty_new INTEGER,
            delta INTEGER,
            history_count INTEGER,
            old_name_alias TEXT,
            old_sku_alias TEXT
        )
    """)
    conn.execute("""
        INSERT INTO anomaly_inbox_new
        (detected_date, sku, item_name, qty_old, qty_new, delta,
         history_count, old_name_alias, old_sku_alias)
        SELECT detected_date, sku, item_name, qty_old, qty_new, delta,
               history_count, old_name_alias, old_sku_alias
        FROM anomaly_inbox
        WHERE id IN (
            SELECT MAX(id) FROM anomaly_inbox GROUP BY item_name
        )
    """)
    old_count = conn.execute("SELECT COUNT(*) FROM anomaly_inbox").fetchone()[0]
    new_count = conn.execute("SELECT COUNT(*) FROM anomaly_inbox_new").fetchone()[0]
    conn.execute("DROP TABLE anomaly_inbox")
    conn.execute("ALTER TABLE anomaly_inbox_new RENAME TO anomaly_inbox")
    conn.execute("DROP INDEX IF EXISTS idx_inbox_active_item")
    conn.commit()

    _log.info('anomaly_inbox migrated: %d -> %d rows (deduplicated)', old_count, new_count)

@functools.lru_cache(maxsize=1)
def load_anomalies() -> pd.DataFrame:
    """Читает все необработанные аномалии из Inbox (одна запись на товар)."""
    if not DB_PATH.exists(): return pd.DataFrame()
    
    update_anomaly_inbox() # Проверяем новые скачки перед загрузкой
    
    with get_connection() as conn:
        df = pd.read_sql_query("""
            SELECT
                sku as 'Артикул',
                item_name as 'Наименование',
                qty_old as 'Было',
                qty_new as 'Стало',
                delta as 'Дельта',
                history_count,
                old_name_alias,
                old_sku_alias
            FROM anomaly_inbox
            ORDER BY detected_date DESC
        """, conn)
        return df


@functools.lru_cache(maxsize=1)
def load_inventory() -> pd.DataFrame:
    """Загружает инвентарь, используя GROUP BY для получения самой свежей записи на товар"""
    if not DB_PATH.exists(): return pd.DataFrame()
    
    with get_connection() as conn:
        # Оптимизированный запрос: база сама отдает по одной самой свежей записи на каждый товар
        # Используем MAX(report_timestamp) в GROUP BY для получения актуальных данных
        query = """
            SELECT
                MAX(id) as 'ID',
                sku as 'Артикул',
                item_name as 'Наименование',
                MAX(price) as 'Цена',
                MAX(quantity) as 'Остаток',
                category as 'Категория',
                MAX(SUBSTR(report_timestamp, 1, 10)) as 'last_seen_date',
                MAX(report_timestamp) as 'report_timestamp'
            FROM stocks
            GROUP BY item_name, sku, category
        """
        df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            # 1. Агрессивная нормализация для поиска дублей
            # Убиваем двойные пробелы, неразрывные пробелы (\xa0), приводим всё в нижний регистр и меняем 'ё' на 'е'
            df['norm_name'] = df['Наименование'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True).str.lower().str.replace('ё', 'е')
            df['norm_sku'] = df['Артикул'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True).str.lower().str.replace('ё', 'е')
            
            # 2. Сортируем так, чтобы самые свежие записи оказались наверху
            df = df.sort_values('report_timestamp', ascending=False)
            
            # 3. Удаляем дубликаты! Оставляем только самую первую (самую свежую) запись для каждого товара
            df = df.drop_duplicates(subset=['norm_name', 'norm_sku'], keep='first')
            
            # 4. Проставляем статусы актуальности
            latest_date = df['last_seen_date'].max()
            df['actual'] = df['last_seen_date'] == latest_date
            
            # 5. Индекс для поиска (используем уже очищенные строки)
            df['_search_index'] = df['norm_name'] + ' ' + df['norm_sku'] + ' ' + df['Категория'].fillna('').astype(str).str.lower()
            
            # Убираем технические колонки, чтобы они не вылезли в интерфейс
            df = df.drop(columns=['report_timestamp', 'norm_name', 'norm_sku'])
            
        return df

@functools.lru_cache(maxsize=4) # Кэшируем несколько статусов
def load_anomaly_report(status="Открыта") -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    with get_connection() as conn:
        # Загружаем аномалии конкретного статуса
        query = "SELECT * FROM anomaly_log WHERE status = :status ORDER BY detected_at DESC"
        return pd.read_sql_query(query, conn, params={"status": status})
    
def save_anomaly_to_db(data: dict):
    """Записывает инцидент в базу и удаляет из Inbox"""
    with get_connection() as conn:
        conn.execute(get_insert_anomaly_query(), data)
        try:
            conn.execute("DELETE FROM anomaly_inbox WHERE item_name = ?", (data['item_name'],))
        except Exception:
            pass
        conn.commit()
    try:
        load_anomalies.cache_clear()
        load_anomaly_report.cache_clear()
    except Exception:
        pass

def close_anomaly_in_db(anomaly_id: int, comment: str):
    with get_connection() as conn:
        conn.execute(get_close_anomaly_query(), {"id": anomaly_id, "comment": comment})
        conn.commit()
    try:
        load_anomalies.cache_clear()
        load_anomaly_report.cache_clear()
    except Exception:
        pass

def cancel_anomaly_in_db(anomaly_id: int, comment: str):
    with get_connection() as conn:
        conn.execute(get_cancel_anomaly_query(), {"id": anomaly_id, "comment": comment})
        conn.commit()
    try:
        load_anomalies.cache_clear()
        load_anomaly_report.cache_clear()
    except Exception:
        pass

@functools.lru_cache(maxsize=1)
def load_dead_stock_analysis() -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    history_depth = CONFIG['database']['history_depth_days']
    with get_connection() as conn:
        query = f"SELECT SUBSTR(report_timestamp, 1, 10) as date, MAX(sku) as sku, category, item_name, price, quantity FROM stocks WHERE report_timestamp >= date('now', '-{history_depth} days') AND item_name IS NOT NULL GROUP BY date, item_name"
        df = pd.read_sql_query(query, conn)
    
    if df.empty: return pd.DataFrame()
    df['date'] = pd.to_datetime(df['date'])
    current = df.sort_values(['item_name', 'date'], ascending=[True, False]).drop_duplicates('item_name').copy()
    current = current[current['quantity'] > 0]
    if current.empty: return pd.DataFrame()
    
    merged = df.merge(current[['item_name', 'quantity']], on='item_name', suffixes=('', '_curr'))
    last_changes = merged[merged['quantity'] != merged['quantity_curr']].sort_values('date', ascending=False).drop_duplicates('item_name')[['item_name', 'date']].rename(columns={'date': 'last_change'})
    res = current.merge(last_changes, on='item_name', how='left')
    
    first_seen = df.groupby('item_name')['date'].min().reset_index(name='first_seen')
    res = res.merge(first_seen, on='item_name', how='left')
    res['last_change'] = res['last_change'].fillna(res['first_seen'])
    res.drop(columns=['first_seen'], inplace=True)
    
    res['Дней без движения'] = (res['date'] - res['last_change']).dt.days.fillna(0).astype(int)
    res['Медиана категории'] = res.groupby('category')['Дней без движения'].transform('median')
    res['Заморожен'] = res['Дней без движения'] > res['Медиана категории']
    res.rename(columns={'sku': 'Артикул', 'item_name': 'Наименование', 'category': 'Категория', 'price': 'Цена', 'quantity': 'Остаток'}, inplace=True)
    return res

@functools.lru_cache(maxsize=256)
def load_velocity_history(item_name: str, sku: str = "") -> pd.DataFrame:
    if not DB_PATH.exists() or not item_name: return pd.DataFrame()
    history_depth = CONFIG['database']['history_depth_days']
    
    # Вспомогательная функция, чтобы не дублировать код
    def fetch_history_for_name(target_n, target_s=""):
        safe_name = str(target_n).strip() if pd.notna(target_n) else ""
        safe_sku = str(target_s).strip() if pd.notna(target_s) else ""
        if safe_sku.lower() in ['nan', 'none', '<na>']: safe_sku = ""
        
        with get_connection() as conn:
            first_word = safe_name.split()[0] if safe_name else ""
            query = f"SELECT item_name, sku, SUBSTR(report_timestamp, 1, 10) as 'Дата', quantity as 'Остаток', report_timestamp FROM stocks WHERE report_timestamp >= date('now', '-{history_depth} days') AND item_name LIKE :fw_pattern"
            df = pd.read_sql_query(query, conn, params={"fw_pattern": f"{first_word}%"})
        
        if not df.empty:
            df['clean_name'] = df['item_name'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True).str.lower().str.replace('ё', 'е')
            df['clean_sku'] = df['sku'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True).str.lower().str.replace('ё', 'е')
            tcn = pd.Series([safe_name]).str.strip().str.replace(r'\s+', ' ', regex=True).str.lower().str.replace('ё', 'е')[0]
            tcs = pd.Series([safe_sku]).str.strip().str.replace(r'\s+', ' ', regex=True).str.lower().str.replace('ё', 'е')[0]
            
            mask = (df['clean_name'] == tcn)
            if tcs: mask &= (df['clean_sku'] == tcs)
            df = df[mask].copy()
            
            if not df.empty:
                df = df.sort_values('report_timestamp', ascending=True).drop_duplicates(subset=['Дата'], keep='last')
                df['Дата'] = pd.to_datetime(df['Дата'])
                return df[['Дата', 'Остаток']].set_index('Дата')
        return pd.DataFrame()

    # 1. Загружаем историю текущего имени
    combined_df = fetch_history_for_name(item_name, sku)
    
    # 2. Ищем алиасы (старые названия) в базе
    with get_connection() as conn:
        try:
            aliases = conn.execute("SELECT old_name FROM item_aliases WHERE new_name = ?", (item_name,)).fetchall()
        except sqlite3.OperationalError:
            aliases = [] # Защита, если таблица еще не создалась
            
    # 3. Подгружаем историю старых названий и сшиваем с новой
    for (old_name,) in aliases:
        alias_df = fetch_history_for_name(old_name, "")
        if not alias_df.empty:
            combined_df = pd.concat([combined_df, alias_df]) if not combined_df.empty else alias_df
            
    # 4. Финальная очистка сшитого графика
    if not combined_df.empty:
        combined_df = combined_df.sort_index()
        # Если в день смены названия есть оба остатка, оставляем самый свежий
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
        
    return combined_df

@functools.lru_cache(maxsize=1)
def get_all_historical_items() -> dict:
    """Выгружает все имена, артикулы и статусы актуальности за всю историю"""
    if not DB_PATH.exists(): return {}
    with get_connection() as conn:
        # Получаем дату последнего парсинга (самую свежую в БД)
        latest_db_date = conn.execute("SELECT MAX(SUBSTR(report_timestamp, 1, 10)) FROM stocks").fetchone()[0]

        # Группируем, чтобы получить уникальные имена, их артикулы и дату последнего появления
        query = """
            SELECT item_name, MAX(sku) as sku, MAX(SUBSTR(report_timestamp, 1, 10)) as last_seen 
            FROM stocks 
            WHERE item_name != '' 
            GROUP BY item_name
        """
        res = conn.execute(query).fetchall()
        
        # Формируем расширенный словарь данных
        result = {}
        for row in res:
            name = row[0]
            sku = row[1] if row[1] else "Без артикула"
            last_seen = row[2]
            # Если дата последней фиксации меньше сегодняшней, значит товар снят с сайта
            is_active = (last_seen == latest_db_date) 
            result[name] = {"sku": sku, "is_active": is_active, "last_seen": last_seen}
            
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  Системные утилиты (вкладка «Система»)
# ─────────────────────────────────────────────────────────────────────────────

@functools.lru_cache(maxsize=1)
def get_parse_days_stats() -> list:
    """
    Возвращает список словарей {'parse_date': str, 'items_count': int}
    за всю историю парсинга, отсортированный от новых к старым.
    Кэшируется — сбрасывать после любых изменений в таблице stocks.
    """
    if not DB_PATH.exists():
        return []
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT DATE(report_timestamp) AS parse_date,
                   COUNT(*)              AS items_count
            FROM stocks
            GROUP BY DATE(report_timestamp)
            ORDER BY parse_date DESC
        """).fetchall()
    return [{"parse_date": row[0], "items_count": row[1]} for row in rows]


def delete_day_data(date: str) -> None:
    """
    Каскадно удаляет все данные за указанный день (формат 'YYYY-MM-DD'):
      - stocks          WHERE DATE(report_timestamp) = date
      - anomaly_inbox   WHERE detected_date = date
      - anomaly_log     WHERE DATE(detected_at) = date
                          AND source = 'Автоматически'
                          AND status = 'Закрыта'

    После удаления немедленно вызывает update_anomaly_inbox() чтобы
    пересчитать дельту между новыми «последними двумя днями» в stocks,
    затем сбрасывает все lru_cache.
    """
    if not DB_PATH.exists():
        return
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM stocks WHERE DATE(report_timestamp) = ?", (date,)
        )
        conn.execute(
            "DELETE FROM anomaly_inbox WHERE detected_date = ?", (date,)
        )
        conn.execute(
            """DELETE FROM anomaly_log
               WHERE DATE(detected_at) = ?
                 AND source = 'Автоматически'
                 AND status = 'Закрыта'""",
            (date,)
        )
        conn.commit()

    # Пересчитываем inbox сразу, чтобы база была консистентна
    update_anomaly_inbox()

    # Сбрасываем ВСЕ lru_cache — любая страница получит свежие данные
    for fn in (
        load_inventory, load_anomalies, load_anomaly_report,
        load_dead_stock_analysis, get_db_stats,
        get_all_historical_items, get_parse_days_stats,
    ):
        try:
            fn.cache_clear()
        except Exception:
            pass

