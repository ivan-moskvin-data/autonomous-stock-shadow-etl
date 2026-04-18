import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path
from contextlib import contextmanager

from queries import get_anomalies_query, get_insert_anomaly_query, get_close_anomaly_query, get_cancel_anomaly_query

# --- НАСТРОЙКИ ПУТЕЙ ---
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"

@contextmanager
def get_connection(): 
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def get_db_stats():
    if not DB_PATH.exists(): return None
    with get_connection() as conn:
        res = conn.execute("SELECT MIN(SUBSTR(report_timestamp, 1, 10)), MAX(SUBSTR(report_timestamp, 1, 10)), COUNT(DISTINCT SUBSTR(report_timestamp, 1, 10)) FROM stocks").fetchone()
        return {"start": res[0], "end": res[1], "days_count": res[2]}

@st.cache_data(ttl=3600)
def load_anomalies() -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    with get_connection() as conn:
        cursor = conn.execute("SELECT DISTINCT SUBSTR(report_timestamp, 1, 10) FROM stocks ORDER BY 1 DESC LIMIT 2")
        dates = [row[0] for row in cursor.fetchall()]
        if len(dates) < 2: return pd.DataFrame()
        
        query = get_anomalies_query()
        df = pd.read_sql_query(query, conn, params={"yesterday": dates[1], "today": dates[0]})
        df.rename(columns={'sku': 'Артикул', 'item_name': 'Наименование', 'qty_old': 'Было', 'qty_new': 'Стало', 'delta': 'Дельта'}, inplace=True)
        return df

@st.cache_data(ttl=3600)
def load_inventory() -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    with get_connection() as conn:
        latest_date = conn.execute("SELECT MAX(SUBSTR(report_timestamp, 1, 10)) FROM stocks").fetchone()[0]
        
        # Берем все данные как есть, без сложной SQL-логики
        query = """
            SELECT 
                id as 'ID', 
                sku as 'Артикул', 
                item_name as 'Наименование', 
                price as 'Цена', 
                quantity as 'Остаток', 
                category as 'Категория',
                SUBSTR(report_timestamp, 1, 10) as 'last_seen_date',
                report_timestamp
            FROM stocks 
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
            df['actual'] = df['last_seen_date'] == latest_date
            
            # 5. Индекс для поиска (используем уже очищенные строки)
            df['_search_index'] = df['norm_name'] + ' ' + df['norm_sku'] + ' ' + df['Категория'].fillna('').astype(str).str.lower()
            
            # Убираем технические колонки, чтобы они не вылезли в интерфейс
            df = df.drop(columns=['report_timestamp', 'norm_name', 'norm_sku'])
            
        return df

@st.cache_data(ttl=60) # Кэшируем на минуту, чтобы не дергать базу постоянно
def load_anomaly_report(status="Открыта") -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    with get_connection() as conn:
        # Загружаем аномалии конкретного статуса
        query = "SELECT * FROM anomaly_log WHERE status = :status ORDER BY detected_at DESC"
        return pd.read_sql_query(query, conn, params={"status": status})
    
def save_anomaly_to_db(data: dict):
    """Записывает инцидент в базу и сбрасывает кэш для обновления экрана"""
    with get_connection() as conn:
        conn.execute(get_insert_anomaly_query(), data)
        conn.commit()
    st.cache_data.clear()

def close_anomaly_in_db(anomaly_id: int, comment: str):
    with get_connection() as conn:
        conn.execute(get_close_anomaly_query(), {"id": anomaly_id, "comment": comment})
        conn.commit()
    st.cache_data.clear()

def cancel_anomaly_in_db(anomaly_id: int, comment: str):
    with get_connection() as conn:
        conn.execute(get_cancel_anomaly_query(), {"id": anomaly_id, "comment": comment})
        conn.commit()
    st.cache_data.clear()

@st.cache_data(ttl=3600)
def load_dead_stock_analysis() -> pd.DataFrame:
    if not DB_PATH.exists(): return pd.DataFrame()
    with get_connection() as conn:
        query = "SELECT SUBSTR(report_timestamp, 1, 10) as date, MAX(sku) as sku, category, item_name, price, quantity FROM stocks WHERE report_timestamp >= date('now', '-365 days') AND item_name IS NOT NULL GROUP BY date, item_name"
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

@st.cache_data(ttl=3600)
def load_velocity_history(item_name: str, sku: str = "") -> pd.DataFrame:
    if not DB_PATH.exists() or not item_name: return pd.DataFrame()
    
    # Вспомогательная функция, чтобы не дублировать код
    def fetch_history_for_name(target_n, target_s=""):
        safe_name = str(target_n).strip() if pd.notna(target_n) else ""
        safe_sku = str(target_s).strip() if pd.notna(target_s) else ""
        if safe_sku.lower() in ['nan', 'none', '<na>']: safe_sku = ""
        
        with get_connection() as conn:
            first_word = safe_name.split()[0] if safe_name else ""
            query = "SELECT item_name, sku, SUBSTR(report_timestamp, 1, 10) as 'Дата', quantity as 'Остаток', report_timestamp FROM stocks WHERE report_timestamp >= date('now', '-365 days') AND item_name LIKE :fw_pattern"
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

@st.cache_data(ttl=3600)
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