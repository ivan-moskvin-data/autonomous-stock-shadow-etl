import sys
import streamlit as st
import pandas as pd
from pathlib import Path
import sqlite3
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parent))
from queries import get_anomalies_query, get_insert_anomaly_query, get_close_anomaly_query, get_cancel_anomaly_query

import math

@st.cache_data(ttl=60, show_spinner=False)
def check_gemini_connection():
    import requests
    try:
        proxies = {
            "http": "socks5://127.0.0.1:1080",
            "https": "socks5://127.0.0.1:1080"
        }
        requests.get("https://generativelanguage.googleapis.com", timeout=1.5)
        return True
    except:
        return False

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def color_rows(row):
    """
    Styler function for Pandas DataFrame to color rows based on anomaly type.
    We use pale, non-distracting colors to maintain focus on data.
    """
    anomaly_type = row['anomaly_type']
    

    colors = {
        'Успешная сверка': 'background-color: rgba(181, 230, 162, 0.4);', # Green
        'Излишек': 'background-color: rgba(255, 230, 156, 0.4);',         # Orange
        'Пересорт (Склад)': 'background-color: rgba(255, 230, 156, 0.4);',# Orange
        'Пересорт (1С)': 'background-color: rgba(255, 230, 156, 0.4);',   # Orange
        'Утеря': 'background-color: rgba(255, 199, 199, 0.4);',           # Red
        'Тихая отмена': 'background-color: rgba(255, 199, 199, 0.4);'     # Red
    }
    
    return [colors.get(anomaly_type, '')] * len(row)

def verify_shadow_forecasts():
    """Обновленная логика: следим за всеми активными прогнозами"""
    with get_connection() as conn:
        # 1. Теперь берем ВСЕ статусы, кроме финальных (Упущенная выгода и Точный прогноз)
        forecasts = pd.read_sql_query("""
            SELECT * FROM ai_forecasts 
            WHERE status NOT IN ('📉 Упущенная выгода', '✅ Точный прогноз', '🔄 Пересчитан ИИ')
        """, conn)
        
        if forecasts.empty: return
        
        latest_inv = load_inventory()
        if latest_inv.empty: return
        
        today = pd.Timestamp.now().normalize()
        
        for _, row in forecasts.iterrows():
            item_name = row['item_name']
            sku = row['sku']
            db_id = row['id']
            
            # УМНЫЙ ПОИСК: Сначала по SKU (он уникален), если нет - по имени
            match = pd.DataFrame()
            if pd.notna(sku) and str(sku).strip():
                match = latest_inv[latest_inv['Артикул'] == sku]
            
            if match.empty:
                match = latest_inv[latest_inv['Наименование'] == item_name]

            if match.empty: continue # Все еще не нашли - пропускаем
            
            curr_qty = float(match.iloc[0]['Остаток'])
            price = float(match.iloc[0]['Цена'])
            avg_sales = float(row['avg_daily_sales'])
            
            # --- ЗАЩИТА ОТ ИИ-ГАЛЛЮЦИНАЦИЙ (37 апреля и т.д.) ---
            pred_date = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
            if pd.isna(pred_date): 
                # Если дата кривая (NaT), ставим безопасную заглушку от "сегодня"
                pred_date = today + pd.Timedelta(days=30)

            # ЖЕСТКАЯ ПРОВЕРКА НА 0 (Твой главный запрос)
            if curr_qty <= 0:
                effective_pred_date = min(today, pred_date)
                days_lost = max(1, (today - effective_pred_date).days)
                lost_value = days_lost * avg_sales * price
                
                conn.execute("""
                    UPDATE ai_forecasts 
                    SET status = '🔴 Товар отсутствует', lost_sales_value = ?, overstock_value = 0 
                    WHERE id = ?
                """, (lost_value, db_id))
                continue

            # Если товар ЕСТЬ, проверяем на Перезатарку (запас > 60 дней)
            if curr_qty > (avg_sales * 60):
                overstock_qty = curr_qty - (avg_sales * 44)
                overstock_value = max(0, overstock_qty * price)
                conn.execute("""
                    UPDATE ai_forecasts 
                    SET status = '🧊 Перезатарка', overstock_value = ?, lost_sales_value = 0 
                    WHERE id = ?
                """, (overstock_value, db_id))
            else:
                # Если остаток в норме, возвращаем в Наблюдение
                conn.execute("UPDATE ai_forecasts SET status = '⏳ Наблюдение' WHERE id = ?", (db_id,))
        
        conn.commit()

# --- НАСТРОЙКИ ПУТЕЙ ---
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"

st.set_page_config(page_title="Stock Shadow | Analytics", page_icon="💎", layout="wide")

# Скрываем лишнее
st.markdown("<style>#MainMenu {visibility: hidden;} footer {visibility: hidden;}</style>", unsafe_allow_html=True)

# --- ИНИЦИАЛИЗАЦИЯ ПАМЯТИ ---
if 'dismissed_names' not in st.session_state:
    st.session_state.dismissed_names = []
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                # Создаем таблицу для хранения связей старых и новых имен
                conn.execute("CREATE TABLE IF NOT EXISTS item_aliases (new_name TEXT, old_name TEXT)")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS expected_deliveries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        item_name TEXT,
                        sku TEXT,
                        qty_expected INTEGER,
                        status TEXT DEFAULT 'Ожидает'
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS ai_forecasts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        item_name TEXT,
                        sku TEXT,
                        predicted_zero_date DATE,
                        recommended_qty INTEGER,
                        reason TEXT,
                        avg_daily_sales REAL,
                        status TEXT DEFAULT '⏳ Наблюдение', -- '⏳ Наблюдение', '📉 Упущенная выгода', '🧊 Перезатарка', '✅ Точный прогноз'
                        lost_sales_value REAL DEFAULT 0,
                        overstock_value REAL DEFAULT 0
                    )
                """)
                conn.commit()
                res = conn.execute("SELECT DISTINCT item_name FROM anomaly_log WHERE detected_at >= datetime('now', '-1 day', 'localtime')").fetchall()
                st.session_state.dismissed_names = [r[0] for r in res]
        except Exception:
            pass

if 'current_page' not in st.session_state:
    st.session_state.current_page = "📦 Склад" 
if 'selected_item_name' not in st.session_state:
    st.session_state.selected_item_name = None

# --- ФУНКЦИИ ЗАГРУЗКИ ---
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
            # ДОБАВЛЕНО: report_timestamp перед FROM
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

# --- ЛОГИКА НАВИГАЦИИ ---
df_inv = load_inventory()
df_anomalies = load_anomalies()
db_stats = get_db_stats()

# Фильтруем активные аномалии по именам
active_anom_count = len(df_anomalies[~df_anomalies['Наименование'].isin(st.session_state.dismissed_names)]) if not df_anomalies.empty else 0

# Безопасно считаем открытые задачи из базы
try:
    with get_connection() as conn:
        open_tasks_count = conn.execute("SELECT COUNT(*) FROM anomaly_log WHERE status = 'Открыта'").fetchone()[0]
except Exception:
    open_tasks_count = 0

with st.sidebar:
    st.title("💎 Autonomous Stock Shadow")
    
    # --- ФУНКЦИЯ ПЕРЕКЛЮЧЕНИЯ (ЗАЩИТА ОТ ЗАЦИКЛИВАНИЯ) ---
    def nav_changed(menu_name):
        if menu_name == "op" and st.session_state.get("op_nav"):
            # Обновляем текущую страницу
            st.session_state.current_page = st.session_state.op_nav.split(' (')[0]
            # Явно приказываем второму меню сбросить выделение
            if "ana_nav" in st.session_state:
                st.session_state.ana_nav = None
                
        elif menu_name == "ana" and st.session_state.get("ana_nav"):
            # Обновляем текущую страницу
            st.session_state.current_page = st.session_state.ana_nav.split(' (')[0]
            # Явно приказываем первому меню сбросить выделение
            if "op_nav" in st.session_state:
                st.session_state.op_nav = None

    # --- ОПРЕДЕЛЯЕМ ТЕКУЩУЮ СТРАНИЦУ ---
    base_page = st.session_state.current_page.split(' (')[0]

    # --- ЛОГИЧЕСКОЕ РАЗДЕЛЕНИЕ МЕНЮ: ОПЕРАЦИИ ---
    st.caption("🛠 ОПЕРАЦИИ")
    op_options = ["📦 Склад", f"⚠️ Аномалии ({active_anom_count})", f"🔥 Задачи ({open_tasks_count})", "📥 Приемка"]
    
    op_idx = next((i for i, opt in enumerate(op_options) if opt.startswith(base_page)), None)
    st.radio("Рабочая область", op_options, index=op_idx, key="op_nav", on_change=nav_changed, args=("op",))
    
    st.write("---")
    
    # --- ЛОГИЧЕСКОЕ РАЗДЕЛЕНИЕ МЕНЮ: АНАЛИТИКА ---
    st.caption("📊 АНАЛИТИКА И KPI")
    ana_options = ["🎯 Эффективность", "❄️ Неликвиды", "📈 Оборачиваемость", "⚖️ A/B Тест: AI vs Человек"]
    
    ana_idx = next((i for i, opt in enumerate(ana_options) if opt.startswith(base_page)), None)
    st.radio("Инструменты анализа", ana_options, index=ana_idx, key="ana_nav", on_change=nav_changed, args=("ana",))

    # --- СИСТЕМНЫЕ КНОПКИ ---
    st.write("---")
    if db_stats:
        st.caption("📂 Статистика базы")
        st.info(f"Дней в базе: {db_stats['days_count']}")
    
    if st.button("🔄 Обновить данные", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
        
    if st.button("🗑️ Очистить легализованные", use_container_width=True, help="Вернуть все скрытые аномалии обратно в список ⚠️"):
        st.session_state.dismissed_names = []
        st.rerun()

# --- СТРАНИЦЫ ---
st.title(f"{st.session_state.current_page}")

COL_RATIOS = [2, 4, 1, 1, 1, 2]
HEADERS_ANOMALIES = ["Артикул", "Наименование", "Было", "Стало", "Δ", "Действие"]

# 1. СТРАНИЦА СКЛАДА
if st.session_state.current_page == "📦 Склад":
    
    # --- CSS ТОЛЬКО ДЛЯ PRIMARY КНОПОК ---
    st.markdown("""
        <style>
        @keyframes blinker { 50% { opacity: 0.6; } }
        /* Таргетируем строго кнопки с типом primary */
        button[data-testid="baseButton-primary"] {
            background-color: #ff4b4b !important;
            color: white !important;
            border: none !important;
            font-weight: bold !important;
            animation: blinker 1.5s linear infinite;
            margin-bottom: 10px;
        }
        </style>
    """, unsafe_allow_html=True)

    # --- ЛОГИКА УМНЫХ БАННЕРОВ ---
    # Считаем задачи в базе
    with get_connection() as conn:
        active_tasks = conn.execute("SELECT COUNT(*) FROM anomaly_log WHERE status = 'Открыта'").fetchone()[0]
    
    # Считаем свежие аномалии (используем уже загруженный датафрейм)
    active_anom = len(df_anomalies[~df_anomalies['Наименование'].isin(st.session_state.dismissed_names)]) if not df_anomalies.empty else 0

    # Выводим баннер для Аномалий, если они есть
    if active_anom > 0:
        if st.button(f"🚨 НОВЫЕ СКАЧКИ ОСТАТКОВ ({active_anom})! Нажми для распределения", type="primary", use_container_width=True, key="banner_anom"):
            st.session_state.current_page = "⚠️ Аномалии"
            st.rerun()

    # Выводим баннер для Задач, если они есть
    if active_tasks > 0:
        if st.button(f"🔥 НЕЗАКРЫТЫЕ ЗАДАЧИ ({active_tasks})! Нажми для проверки на полке", type="primary", use_container_width=True, key="banner_tasks"):
            st.session_state.current_page = "🔥 Задачи"
            st.rerun()

    # --- ГЛОБАЛЬНАЯ СИСТЕМА УВЕДОМЛЕНИЙ ОБ ОТЛОЖЕННОМ ИИ ---
    pending_flag = Path("logs/ai_pending.flag")
    if pending_flag.exists():
        is_proxy_ok = check_gemini_connection()
        if not is_proxy_ok:
            st.error("🚨 **Системное предупреждение:** Парсер собрал новые данные, но ИИ-прогнозы не построены (нет связи с Gemini API). **Пожалуйста, включите VPN/Прокси!**")
        else:
            st.warning("⚠️ **ИИ ожидает запуска:** В системе есть свежие не проанализированные данные. Включите прокси, перейдите на вкладку '⚖️ A/B Тест' и нажмите кнопку запуска.")

    st.write("---")
    
    search = st.text_input("🔍 Поиск", placeholder="Артикул или название...")
    if search:
        query_words = search.lower().replace('ё', 'е').split()
        mask = pd.Series(True, index=df_inv.index)
        for word in query_words: mask &= df_inv['_search_index'].str.contains(word, regex=False)
        f_df = df_inv[mask].drop(columns=['_search_index'])
        
        if 0 < len(f_df) <= 50:
            cols = st.columns([2, 4, 1, 1, 2])
            for i, h in enumerate(["Артикул", "Наименование", "Цена", "Остаток", "Анализ"]): cols[i].write(f"**{h}**")
            st.divider()
            for idx, row in f_df.iterrows():
                c = st.columns([2, 4, 1, 1, 2])
                display_name = row['Наименование']
                if not row['actual']:
                    display_name = f"🔘 {display_name} ❌(Снят с сайта {row['last_seen_date']})"
                
                c[0].write(row['Артикул'])
                c[1].write(display_name)
                c[2].write(f"{row['Цена']:.0f} ₽")
                c[3].write(f"{row['Остаток']} шт.")
                
                # ТРИ КНОПКИ В КОЛОНКЕ (📈 График, ⚠️ Ошибка, ✅ Всё ок)
                btn_c = c[4].columns(3)
                
                if btn_c[0].button("📈", key=f"v_{row['ID']}", help="График оборачиваемости"):
                    st.session_state.selected_item_name = row['Наименование']
                    st.session_state.selected_item_sku = row['Артикул'] 
                    st.session_state.current_page = "📈 Оборачиваемость"
                    st.rerun()
                
                if btn_c[1].button("⚠️", key=f"err_{row['ID']}", help="Зафиксировать расхождение"):
                    st.session_state.manual_anomaly_id = row['ID']
                    st.rerun()

                # ФИКСАЦИЯ УСПЕШНОЙ СВЕРКИ (Экономия похода в офис)
                if btn_c[2].button("✅", key=f"ok_{row['ID']}", help="Остаток сошелся"):
                    save_anomaly_to_db({
                        "item_name": row['Наименование'],
                        "anomaly_type": "Успешная сверка",
                        "qty_system": row['Остаток'],
                        "qty_physical": row['Остаток'],
                        "financial_impact": 0,
                        "source": "Вручную (План)",
                        "status": "Закрыта",
                        "comment": "Сверено с планшета. Всё ок."
                    })
                    st.toast("✅ Сверка подтверждена! Экономия зафиксирована.")

                # Если нажали на ⚠️, показываем поле ввода
                if st.session_state.get('manual_anomaly_id') == row['ID']:
                    fact_qty = st.number_input("Реальный остаток:", min_value=0, value=int(row['Остаток']), key=f"num_{row['ID']}")
                    
                    is_planned = st.checkbox("⚙️ Плановая проверка (циклическая инвентаризация)", value=True, key=f"check_type_{row['ID']}")
                    
                    # 🧪 НОВАЯ ГАЛОЧКА ДЛЯ ТЕСТОВ
                    is_test = st.checkbox("🧪 Тестовая запись (исключить из аналитики)", value=False, key=f"test_{row['ID']}")
                    
                    user_comment = st.text_input("Заметка (по желанию):", placeholder="Напр: резерв или пересорт", key=f"manual_com_{row['ID']}")
                    
                    if st.button("✅ Подтвердить", key=f"conf_{row['ID']}"):
                        source_type = "Вручную (План)" if is_planned else "Вручную (Инцидент)"
                        
                        # Меняем тип аномалии, если это тест
                        anom_type = "Тестовая запись" if is_test else "Ручная проверка"
                        # Обнуляем ущерб, если это тест
                        impact = 0 if is_test else abs(row['Остаток'] - fact_qty) * row['Цена']
                        
                        save_anomaly_to_db({
                            "item_name": row['Наименование'],
                            "anomaly_type": anom_type,
                            "qty_system": row['Остаток'],
                            "qty_physical": fact_qty,
                            "financial_impact": impact,
                            "source": source_type,
                            "status": "Открыта",
                            "comment": user_comment
                        })
                        st.session_state.manual_anomaly_id = None
                        st.rerun()
                    if st.button("❌", key=f"can_{row['ID']}"):
                        st.session_state.manual_anomaly_id = None
                        st.rerun()
        else:
            st.dataframe(f_df.drop(columns=['ID', 'Категория']), use_container_width=True, height=500, hide_index=True)
    else: 
        st.info("👆 Введите артикул или название для поиска. Ниже — статус системы.")
        st.write("---")
        st.subheader("🤖 Мониторинг парсера (Data Health)")
        
        with get_connection() as conn:
            # Запрос статистики за последние 3 дня
            query_stats = """
                SELECT 
                    DATE(report_timestamp) as parse_date,
                    COUNT(*) as items_count,
                    MIN(report_timestamp) as start_time,
                    MAX(report_timestamp) as end_time
                FROM stocks 
                GROUP BY DATE(report_timestamp)
                ORDER BY parse_date DESC 
                LIMIT 3
            """
            df_stats = pd.read_sql_query(query_stats, conn)
            
        if df_stats.empty:
            st.warning("В базе данных еще нет записей.")
        else:
            import os
            from datetime import datetime
            
            latest = df_stats.iloc[0]
            
            # 1. Расчет дельты (изменения количества товаров)
            delta_text = "Первый запуск"
            if len(df_stats) > 1:
                prev_count = df_stats.iloc[1]['items_count']
                delta_val = int(latest['items_count'] - prev_count)
                delta_text = f"{delta_val:+} шт."

            # 2. Расчет длительности парсинга
            fmt = "%Y-%m-%d %H:%M:%S"
            try:
                start_dt = datetime.strptime(latest['start_time'], fmt)
                end_dt = datetime.strptime(latest['end_time'], fmt)
                duration_seconds = (end_dt - start_dt).total_seconds()
                duration_minutes = round(duration_seconds / 60)
                
                if duration_minutes > 0:
                    dur_display = f"{duration_minutes} мин."
                else:
                    dur_display = f"{int(duration_seconds)} сек."
            except Exception:
                dur_display = "н/д"

            # 3. Проверка статуса (Надежный поиск через psutil)
            import psutil

            is_running = False
            # Перебираем все процессы в оперативной памяти
            for proc in psutil.process_iter(['cmdline']):
                try:
                    cmd = proc.info.get('cmdline')
                    # Ищем процесс, в команде запуска которого есть 'parser.py'
                    if cmd and any('parser.py' in str(arg).lower() for arg in cmd):
                        is_running = True
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    # Игнорируем системные процессы, к которым нет доступа
                    pass

            # --- МЕТРИКИ (ВЕРХНИЙ РЯД) ---
            c1, c2, c3 = st.columns([1, 1, 1.5])

            c1.metric("Собрано товаров", f"{latest['items_count']} шт.", delta=delta_text)
            c2.metric("Длительность", dur_display, help="Разница между первой и последней записью в БД за день.")

            with c3:
                st.write("**Статус системы**")
                if is_running:
                    # Яркий индикатор реального процесса
                    st.warning("🔄 **В процессе парсинга...**")
                else:
                    st.success("✅ Завершен успешно")
                    
                # UX Улучшение: кнопка ручного обновления
                if st.button("🔄 Обновить статус", use_container_width=True):
                    st.rerun()

            st.write("---")
            
            # --- ТАБЛИЦА ДИНАМИКИ (БЕЗ ИНДЕКСА) ---
            st.write(f"**📊 Динамика за последние {len(df_stats)} дн.**")
            
            # Подготовка данных для таблицы
            display_df = df_stats.copy()
            display_df['Время начала'] = display_df['start_time'].str[11:19]
            display_df['Время конца'] = display_df['end_time'].str[11:19]
            display_df['Всего SKU'] = display_df['items_count']
            
            plot_df = display_df[['parse_date', 'Всего SKU', 'Время начала', 'Время конца']].rename(columns={'parse_date': 'Дата'})
            
            # Используем st.dataframe для скрытия индекса
            st.dataframe(
                plot_df,
                use_container_width=True,
                hide_index=True, # Это уберет первую безымянную колонку
                column_config={
                    "Дата": st.column_config.TextColumn("Дата"),
                    "Всего SKU": st.column_config.NumberColumn("Всего SKU"),
                    "Время начала": st.column_config.TextColumn("Время начала"),
                    "Время конца": st.column_config.TextColumn("Время конца")
                }
            )

            # --- НОВЫЙ БЛОК: ИСЧЕЗНУВШИЕ ТОВАРЫ ---
            if len(df_stats) > 1:
                yesterday_date = df_stats.iloc[1]['parse_date']
                
                # Ищем товары, которые парсер видел вчера, но не увидел сегодня
                lost_items = df_inv[(df_inv['last_seen_date'] == yesterday_date) & (~df_inv['actual'])]
                
                if not lost_items.empty:
                    with st.expander(f"📉 Сняты с сайта при последнем парсинге ({len(lost_items)} шт.)"):
                        st.caption(f"Эти позиции были в выгрузке за {yesterday_date}, но сегодня отсутствуют.")
                        display_lost = lost_items[['Артикул', 'Наименование', 'Цена', 'Остаток']].copy()
                        
                        st.dataframe(
                            display_lost, 
                            use_container_width=True, 
                            hide_index=True,
                            column_config={
                                "Цена": st.column_config.NumberColumn(format="%d ₽"),
                                "Остаток": st.column_config.NumberColumn(format="%d шт.")
                            }
                        )
                else:
                    st.success("✅ С момента прошлого парсинга ни один товар не пропал с сайта.")

# 2. СТРАНИЦА АНОМАЛИЙ
elif st.session_state.current_page == "⚠️ Аномалии":
    active_anom = df_anomalies[~df_anomalies['Наименование'].isin(st.session_state.dismissed_names)] if not df_anomalies.empty else pd.DataFrame()

    if not active_anom.empty:
        # Берем только приходы (Дельта > 0)
        arrivals = active_anom[active_anom['Дельта'] > 0]
        
        if not arrivals.empty:
            with get_connection() as conn:
                # Загружаем ожидаемые приходы
                expected = pd.read_sql_query("SELECT * FROM expected_deliveries WHERE status = 'Ожидает'", conn)
                
                if not expected.empty:
                    for idx, anom_row in arrivals.iterrows():
                        # Ищем совпадение по имени ИЛИ артикулу И количеству
                        match = expected[
                            ((expected['item_name'] == anom_row['Наименование']) | (expected['sku'] == anom_row['Артикул'])) & 
                            (expected['qty_expected'] == anom_row['Дельта'])
                        ]
                        
                        if not match.empty:
                            match_id = match.iloc[0]['id']
                            
                            # Нашли! Сами легализуем аномалию
                            save_anomaly_to_db({
                                "item_name": anom_row['Наименование'],
                                "anomaly_type": "📦 Плановый приход",
                                "qty_system": anom_row['Стало'],
                                "qty_physical": anom_row['Было'], 
                                "financial_impact": 0,
                                "source": "Автоматически (Нейро-приемка)",
                                "status": "Закрыта", 
                                "comment": f"Авто-матчинг с накладной #{match_id}"
                            })
                            
                            # Помечаем в буфере, что этот товар принят
                            conn.execute("UPDATE expected_deliveries SET status = 'Принято' WHERE id = ?", (int(match_id),))
                            conn.commit()
                            
                            # Скрываем с экрана
                            st.session_state.dismissed_names.append(anom_row['Наименование'])
                            st.toast(f"🤖 Авто-приемка: {anom_row['Наименование']}")
                            st.rerun()

    if active_anom.empty: 
        st.success("Аномалий нет.")
    else:
        cols = st.columns(COL_RATIOS)
        for i, h in enumerate(HEADERS_ANOMALIES): cols[i].write(f"**{h}**")
        st.divider()
        for idx, row in active_anom.iterrows():
            with st.container():
                c = st.columns(COL_RATIOS)
                c[0].write(row['Артикул'])
                c[1].write(row['Наименование'])
                c[2].write(row['Было'])
                c[3].write(row['Стало'])
                c[4].write(f":green[+{row['Дельта']}]")

                # Умная группировка кнопок (Сетка 3x3 вместо одной длинной строки)
                # Ряд 1: Негативные инциденты (Потери и сбои)
                row1 = [("Утеря", "минус"), ("Тихая отмена", "отмена"), ("Системная ошибка", "sys_err")]
                # Ряд 2: Пересорты и излишки (Смещение остатков)
                row2 = [("Пересорт (Склад)", "склад"), ("Пересорт (1С)", "офис"), ("Излишек", "плюс")]
                # Ряд 3: Рутина и автоматизация (Системные корректировки)
                row3 = [("📦 Плановый приход", "delivery"), ("⏳ Догруз с сайта", "late_sync"), ("🔄 Обновление карточки", "card_update")]
                
                grid = [row1, row2, row3]

                # Отрисовываем сетку
                for button_row in grid:
                    btn_cols = st.columns(len(button_row))
                    for i, (label, key_suffix) in enumerate(button_row):
                        
                        # Наша кнопка вызова меню склейки
                        if label == "🔄 Обновление карточки":
                            if btn_cols[i].button(label, key=f"anom_{idx}_{key_suffix}", use_container_width=True):
                                st.session_state.link_target_idx = idx
                                st.rerun()
                        else:
                            # Логика для всех остальных обычных кнопок
                            if btn_cols[i].button(label, key=f"anom_{idx}_{key_suffix}", use_container_width=True):
                                price = df_inv[df_inv['Наименование'] == row['Наименование']]['Цена'].values[0] if not df_inv.empty else 0
                                final_status = "Закрыта" if label in ["Системная ошибка", "📦 Плановый приход", "⏳ Догруз с сайта"] else "Открыта"
                                
                                auto_comment = ""
                                if label == "📦 Плановый приход": auto_comment = "Штатное поступление товара"
                                elif label == "⏳ Догруз с сайта": auto_comment = "Запоздалая выгрузка остатков витрины"
                                
                                anomaly_data = {
                                    "item_name": row['Наименование'],
                                    "anomaly_type": label,
                                    "qty_system": row['Стало'],
                                    "qty_physical": row['Было'], 
                                    "financial_impact": abs(row['Дельта'] * price) if label not in ["Системная ошибка", "📦 Плановый приход", "⏳ Догруз с сайта"] else 0,
                                    "source": "Автоматически",
                                    "status": final_status, 
                                    "comment": auto_comment
                                }
                                save_anomaly_to_db(anomaly_data)
                                st.session_state.dismissed_names.append(row['Наименование'])
                                st.success(f"Зафиксировано: {label}")
                                st.rerun()

                # --- МЕНЮ СКЛЕЙКИ ИСТОРИИ (КАК НА СКЛАДЕ) ---
                if st.session_state.get('link_target_idx') == idx:
                    st.write("---")
                    
                    # Верхний ряд с кнопками пропуска и отмены (чтобы всегда были под рукой)
                    col_top1, col_top2 = st.columns([3, 1])
                    if col_top1.button("⏭️ Просто обновить карточку (БЕЗ склейки с историей)", key=f"skip_link_{idx}"):
                        save_anomaly_to_db({
                            "item_name": row['Наименование'],
                            "anomaly_type": "🔄 Обновление карточки",
                            "qty_system": row['Стало'],
                            "qty_physical": row['Было'], 
                            "financial_impact": 0,
                            "source": "Автоматически",
                            "status": "Закрыта", 
                            "comment": "Изменилось название на сайте"
                        })
                        st.session_state.dismissed_names.append(row['Наименование'])
                        st.session_state.link_target_idx = None
                        st.rerun()
                        
                    if col_top2.button("❌ Отмена", key=f"cancel_link_{idx}"):
                        st.session_state.link_target_idx = None
                        st.rerun()

                    st.write("") # Небольшой отступ

                    # 1. Поле ручного поиска (один в один как на складе)
                    search_query = st.text_input("🔍 Поиск старой карточки для привязки:", 
                                                placeholder="Артикул или название...",
                                                key=f"search_link_{idx}")

                    matched_df = pd.DataFrame()
                    
                    # 2. Движок поиска от вкладки Склад
                    if search_query:
                        import re
                        # Убиваем мусорные значки
                        clean_query = re.sub(r'\(снят с сайта.*?\)', '', search_query, flags=re.IGNORECASE)
                        clean_query = clean_query.replace('🔘', '').replace('❌', '').strip()
                        query_words = clean_query.lower().replace('ё', 'е').split()
                        
                        if query_words:
                            mask = pd.Series(True, index=df_inv.index)
                            for word in query_words: 
                                mask &= df_inv['_search_index'].str.contains(word, regex=False)
                            
                            matched_df = df_inv[mask].copy()
                            # ПРИОРИТЕТ: Сортируем так, чтобы неактивные (снятые с сайта) были на самом верху
                            matched_df = matched_df.sort_values(by='actual', ascending=True).head(30)
                            st.caption(f"🔍 Найдено: {len(df_inv[mask])}. Показаны первые 30.")
                    else:
                        # Если ничего не введено, показываем кандидатов, пропавших с сайта
                        today_lost = df_anomalies[
                            (df_anomalies['Дельта'] < 0) & 
                            (~df_anomalies['Наименование'].isin(st.session_state.dismissed_names))
                        ]['Наименование'].tolist()
                        
                        mask1 = df_inv['Наименование'].isin(today_lost)
                        mask2 = ~df_inv['actual']
                        matched_df = df_inv[mask1 | mask2].sort_values(by='actual', ascending=True).head(10).copy()
                        st.caption("Показаны недавно пропавшие товары. Используйте поиск, чтобы найти другие.")

                    # 3. ОТРИСОВКА РЕЗУЛЬТАТОВ КАК НА СКЛАДЕ (Таблица вместо списка)
                    if not matched_df.empty:
                        hc = st.columns([2, 4, 2, 2])
                        for i, h in enumerate(["Артикул", "Наименование", "Статус", "Действие"]): 
                            hc[i].write(f"**{h}**")
                        st.divider()
                        
                        for matched_idx, m_row in matched_df.iterrows():
                            c = st.columns([2, 4, 2, 2])
                            c[0].write(m_row['Артикул'])
                            
                            display_name = m_row['Наименование']
                            if not m_row['actual']:
                                c[1].write(f"🔘 {display_name}")
                                c[2].write(f"❌ Снят ({m_row['last_seen_date']})")
                            else:
                                c[1].write(display_name)
                                c[2].write("✅ Активен")
                                
                            # Кнопка склейки прямо в строке товара!
                            if c[3].button("🔗 Склеить", key=f"do_link_{idx}_{matched_idx}", type="primary"):
                                old_name = m_row['Наименование']
                                with get_connection() as conn:
                                    conn.execute("INSERT INTO item_aliases (new_name, old_name) VALUES (?, ?)", (row['Наименование'], old_name))
                                    conn.execute("""
                                        INSERT INTO anomaly_log (detected_at, item_name, anomaly_type, qty_system, qty_physical, financial_impact, source, status, comment)
                                        VALUES (datetime('now', 'localtime'), ?, '🔄 Обновление карточки', 0, 0, 0, 'Автоматически', 'Закрыта', ?)
                                    """, (old_name, f"🔗 Склеено (старое имя). Новое: {row['Наименование']}"))
                                    conn.commit()
                                if old_name not in st.session_state.dismissed_names:
                                    st.session_state.dismissed_names.append(old_name)

                                save_anomaly_to_db({
                                    "item_name": row['Наименование'],
                                    "anomaly_type": "🔄 Обновление карточки",
                                    "qty_system": row['Стало'],
                                    "qty_physical": row['Было'], 
                                    "financial_impact": 0,
                                    "source": "Автоматически",
                                    "status": "Закрыта", 
                                    "comment": f"Склейка: {old_name}"
                                })
                                st.session_state.dismissed_names.append(row['Наименование'])
                                st.session_state.link_target_idx = None
                                st.rerun()
                            st.divider()
                    else:
                        st.info("По вашему запросу ничего не найдено.")
            st.divider()

# 3. СТРАНИЦА ЭФФЕКТИВНОСТИ И KPI (бывший Архив)
elif st.session_state.current_page == "🎯 Эффективность":
    
    # 🧪 ПЕРЕКЛЮЧАТЕЛЬ DEV MODE
    hc1, hc2 = st.columns([3, 1])
    hc1.subheader("🎯 KPI: Эффективность и Качество (Lean Model)")
    include_tests = hc2.checkbox("🧪 Тестовые данные и баги", value=False, help="Показать тестовые записи для отладки")
    
    with get_connection() as conn:
        # Добавляем status, detected_at и resolved_at для расчета MTTR
        query = """
            SELECT item_name, source, anomaly_type, status, detected_at, resolved_at 
            FROM anomaly_log 
            WHERE anomaly_type NOT IN ('Тестовая запись', 'Системная ошибка', '📦 Плановый приход', '⏳ Догруз с сайта', '🔄 Обновление карточки')
        """
        if include_tests:
            query = "SELECT item_name, source, anomaly_type, status, detected_at, resolved_at FROM anomaly_log"
        df_kpi = pd.read_sql_query(query, conn)
        
    if df_kpi.empty:
        st.info("Пока нет данных для расчета KPI.")
    else:
        # Получаем актуальный список неликвидов для сопоставления
        df_dead = load_dead_stock_analysis()
        # Создаем словарь: Название -> Статус заморозки
        dead_map = dict(zip(df_dead['Наименование'], df_dead['Заморожен'])) if not df_dead.empty else {}

        # Функция определения "индивидуального риска" для каждой строки
        def get_item_risk_days(row):
            if row['anomaly_type'] == 'Успешная сверка': return 0 # Сверки не считаем в риск
            
            is_frozen = dead_map.get(row['item_name'], False)
            if is_frozen:
                return 365  # Для неликвида риск — год
            else:
                return 90   # Для обычного товара — квартал (усредненно)

        # Считаем риск для каждой найденной проактивной аномалии
        proactive_df = df_kpi[
            (df_kpi['source'].isin(['Автоматически', 'Вручную (План)'])) & 
            (df_kpi['anomaly_type'] != 'Успешная сверка')
        ].copy()
        
        # Применяем веса
        if not proactive_df.empty:
            proactive_df['risk_days'] = proactive_df.apply(get_item_risk_days, axis=1)
            # Итого спасенных дней = Сумма (Риск товара - 1 день на обнаружение системой)
            total_risk_days_saved = (proactive_df['risk_days'] - 1).sum()
            proactive_issues = len(proactive_df)
        else:
            total_risk_days_saved = 0
            proactive_issues = 0
        
        # --- 1. ПАРАМЕТРЫ РУТИНЫ (Шаги) ---
        OVERHEAD_MINUTES = 20  
        MEDIAN_BATCH_SIZE = 17 
        min_per_item = OVERHEAD_MINUTES / MEDIAN_BATCH_SIZE
        
        # --- 2. ПАРАМЕТРЫ КОММУНИКАЦИЙ (Activity-Based Costing) ---
        TIME_WAREHOUSE_WAITING = 10 # мин. простоя кладовщика (дозвон, ожидание ответа, переупаковка)
        TIME_OFFICE_INVESTIGATION = 10 # мин. работы офиса (поиск в 1С, исправление документов)
        
        # Общая стоимость одной эскалации в человеко-минутах
        ESCALATION_COST_MIN = TIME_WAREHOUSE_WAITING + TIME_OFFICE_INVESTIGATION
        
        # --- 3. ПАРАМЕТРЫ OPEX (Печать) ---
        # Средняя стоимость 1 листа А4: Бумага (~0.6₽) + Тонер (~0.4₽) + Амортизация принтера (~0.2₽)
        COST_PER_SHEET_RUB = 1.2 
        opex_per_item = COST_PER_SHEET_RUB / MEDIAN_BATCH_SIZE

        # --- 4. ПАРАМЕТРЫ ЗАДЕРЖКИ (Information Latency) ---
        # Если бы не система, ошибка висела бы в среднем 90 дней (цикл полной сверки)
        AVG_MANUAL_DETECTION_DAYS = 90 

        # --- 5. РАСЧЕТ MEDIAN TIME TO RESOLVE (MTTR) ---
        resolved_tasks = df_kpi[
            (df_kpi['status'] == 'Закрыта') & 
            (df_kpi['anomaly_type'] != 'Успешная сверка') & 
            (df_kpi['detected_at'].notnull()) & 
            (df_kpi['resolved_at'].notnull())
        ].copy()
        
        if not resolved_tasks.empty:
            resolved_tasks['detected_at'] = pd.to_datetime(resolved_tasks['detected_at'])
            resolved_tasks['resolved_at'] = pd.to_datetime(resolved_tasks['resolved_at'])
            # Считаем разницу и берем МЕДИАНУ вместо среднего
            resolve_times = (resolved_tasks['resolved_at'] - resolved_tasks['detected_at']).dt.total_seconds() / 3600.0
            mttr_median = resolve_times[resolve_times > 0].median() 
            if pd.isna(mttr_median): mttr_median = 0.0
        else:
            mttr_median = 0.0
            
        # Форматирование вывода
        if 0 < mttr_median < 1:
            mttr_display = f"{mttr_median * 60:.0f} мин."
        else:
            mttr_display = f"{mttr_median:.1f} ч."

        # ОПРЕДЕЛЕНИЕ ЦВЕТА И НОРМЫ (SLA)
        # Норма для склада: закрытие аномалии в течение 4-х часов (одна рабочая смена)
        S_MTTR_NORM = 8.0 
        mttr_delta_color = "normal" if mttr_median <= S_MTTR_NORM else "inverse"

        # --- РАСЧЕТЫ (Порядок важен для предотвращения NameError) ---
        total_checks = len(df_kpi)
        
        # Сначала считаем количество реальных проблем, найденных ДО прихода клиента
        proactive_issues = len(df_kpi[
            (df_kpi['source'].isin(['Автоматически', 'Вручную (План)'])) & 
            (df_kpi['anomaly_type'] != 'Успешная сверка')
        ])
        
        # Теперь считаем все производные метрики
        routine_saved_hours = (total_checks * min_per_item) / 60
        communication_saved_hours = (proactive_issues * ESCALATION_COST_MIN) / 60
        total_saved_hours = routine_saved_hours + communication_saved_hours
        
        total_opex_saved = total_checks * opex_per_item
        sheets_saved = total_checks / MEDIAN_BATCH_SIZE
        trees_saved = sheets_saved / 10000 # 1 дерево ≈ 10 000 листов А4
        
        # Суммарно предотвращено дней риска (на базе уже рассчитанного proactive_issues)
        total_risk_days_saved = proactive_issues * (AVG_MANUAL_DETECTION_DAYS - 1)
        
        proactive_count = len(df_kpi[df_kpi['source'].isin(['Автоматически', 'Вручную (План)'])])
        proactive_rate = (proactive_count / total_checks) * 100 if total_checks > 0 else 0

        # --- ПОДГОТОВКА ФОРМАТА ВРЕМЕНИ ---
        display_h = int(total_saved_hours)
        display_m = int(round((total_saved_hours - display_h) * 60))
        # Обработка случая, когда округление дает 60 минут
        if display_m == 60:
            display_h += 1
            display_m = 0
        time_str = f"{display_h} ч. {display_m} мин."

        # --- ОТРИСОВКА ДАШБОРДА (Сетка 3х2 с обновленным неймингом) ---
        
        # Строка 1: Управление рисками и временем
        r1_c1, r1_c2, r1_c3 = st.columns(3)
        
        with r1_c1:
            st.metric("Risk - Предотвращено риска", f"{total_risk_days_saved:,.0f} дн.".replace(',', ' '), 
                      delta="MTTD: <24ч", delta_color="off",
                      help=f"Суммарный лаг обнаружения. 365 дн. для неликвидов и {AVG_MANUAL_DETECTION_DAYS} дн. для активного стока.")
            
        with r1_c2:
            # Передвинули MTTR на второе место первой строки
            st.metric("MTTR - Время устранения", mttr_display, 
                      delta=f"SLA: {S_MTTR_NORM}ч", delta_color=mttr_delta_color,
                      help=f"""
                      Median Time to Resolve: показатель операционной дисциплины. 
                      Показывает типичное время реакции склада на проблему. 
                      Норма (SLA): до {S_MTTR_NORM} часов.
                      """)
            
        with r1_c3:
            st.metric("Time - Сэкономлено времени", time_str, 
                      help=f"Рутина: {routine_saved_hours:.2f}ч + Коммуникации: {communication_saved_hours:.2f}ч")

        st.write("") # Отступ между строками

        # Строка 2: Качество, Финансы и Экология
        r2_c1, r2_c2, r2_c3 = st.columns(3)
        
        with r2_c1:
            # Передвинули Proactive Rate на первое место второй строки
            st.metric("PR - Проактивность", f"{proactive_rate:.1f}%")
            st.progress(proactive_rate / 100.0)
            
        with r2_c2:
            st.metric("OPEX - Снижение затрат", f"{total_opex_saved:.2f} ₽", 
                      help=f"Сэкономлено {sheets_saved:.2f} листов А4. Расчет: {COST_PER_SHEET_RUB}₽/лист")
            
        with r2_c3:
            st.metric("ESG - Eco Impact", f"{trees_saved:.5f} 🌳", 
                      help=f"Сохранено деревьев исходя из объема нераспечатанной бумаги ({sheets_saved:.2f} стр.)")

    st.divider()

    # --- РАЗДЕЛ 1: SYSTEM IQ (Дневная динамика) ---
    st.subheader("🤖 System IQ (Daily Health & Intel)")
    with get_connection() as conn:
        # Прячем тесты, если галочка не стоит
        iq_where = "" if include_tests else "WHERE anomaly_type != 'Тестовая запись'"
        
        # 1. Выделяем ИИ в отдельную категорию по полю source!
        iq_query = f"""
            SELECT 
                DATE(detected_at) as Day, 
                CASE 
                    WHEN IFNULL(comment, '') LIKE '%[BUG]%' THEN 'Failures'
                    WHEN source = 'Автоматически (Нейро-приемка)' THEN '✨ AI Auto-Receive'
                    WHEN anomaly_type IN ('📦 Плановый приход', '⏳ Догруз с сайта', '🔄 Обновление карточки') THEN 'Routine (Manual)'
                    WHEN anomaly_type IN ('Системная ошибка') THEN 'Failures'
                    WHEN anomaly_type IN ('Тестовая запись') THEN 'Debug'
                    ELSE 'Signal (Anomalies)'
                END as cat,
                COUNT(*) as count
            FROM anomaly_log
            {iq_where}
            GROUP BY 1, 2
            ORDER BY Day ASC 
        """
        df_iq = pd.read_sql_query(iq_query, conn)
    
    if not df_iq.empty:
        all_cats = sorted(df_iq['cat'].unique().tolist())
        selected_cats = st.multiselect(
            "🔎 Детализация System IQ (выберите категории для сравнения):", 
            options=all_cats, 
            default=all_cats,
            key="iq_filter"
        )
        
        df_iq_filtered = df_iq[df_iq['cat'].isin(selected_cats)]
        
        if not df_iq_filtered.empty:
            chart_iq = df_iq_filtered.pivot(index='Day', columns='cat', values='count').fillna(0)
            
            # 2. Добавляем фиолетовый цвет для ИИ
            color_map_iq = {
                'Routine (Manual)': '#3498db', # Синий (рутина)
                '✨ AI Auto-Receive': '#9b59b6', # Фиолетовый (работа ИИ)
                'Debug': '#95a5a6',            # Серый
                'Failures': '#e74c3c',         # Красный
                'Signal (Anomalies)': '#2ecc71' # Зеленый
            }
            
            current_colors = [color_map_iq.get(col, '#000000') for col in chart_iq.columns]
            
            st.area_chart(chart_iq, color=current_colors) 
            st.caption("🟣 **AI Auto-Receive:** Отработано нейросетью | 🔵 **Routine (Manual):** Ручные клики менеджера | 🟢 **Signal:** Аномалии")
        else:
            st.warning("Выберите хотя бы одну категорию для отображения графика.")

    st.write("---") 

    # --- РАЗДЕЛ 2: FEATURE ADOPTION (Дневная динамика UX) ---
    st.subheader("🖱️ Feature Adoption (Ручная нагрузка на менеджера)")
    with get_connection() as conn:
        # 3. ИСКЛЮЧАЕМ РАБОТУ ИИ ИЗ ЭТОГО ГРАФИКА (Здесь только ручные клики!)
        ad_where = "WHERE anomaly_type != 'Успешная сверка' AND IFNULL(comment, '') NOT LIKE '%[BUG]%' AND source != 'Автоматически (Нейро-приемка)'"
        if not include_tests:
            ad_where += " AND anomaly_type NOT IN ('Тестовая запись')"
        
        adoption_ts_query = f"""
            SELECT 
                DATE(detected_at) as Day, 
                anomaly_type,
                COUNT(*) as count
            FROM anomaly_log
            {ad_where}
            GROUP BY 1, 2
            ORDER BY Day ASC 
        """
        df_adoption_ts = pd.read_sql_query(adoption_ts_query, conn)
    
    if not df_adoption_ts.empty:
        all_types = sorted(df_adoption_ts['anomaly_type'].unique().tolist())
        selected_types = st.multiselect(
            "🔎 Динамика кликов (успех внедрения ИИ виден по падению кнопки 'Плановый приход'):", 
            options=all_types, 
            default=all_types,
            key="ad_filter"
        )
        
        df_ad_filtered = df_adoption_ts[df_adoption_ts['anomaly_type'].isin(selected_types)]
        
        if not df_ad_filtered.empty:
            chart_adoption = df_ad_filtered.pivot(index='Day', columns='anomaly_type', values='count').fillna(0)
            st.area_chart(chart_adoption)
            st.caption("График показывает, сколько раз человек физически нажимал кнопки классификации. Внедрение автоматизации снижает эти показатели.")
        else:
            st.warning("Выберите типы кнопок для отображения.")
    
    st.divider()

    st.subheader("📜 История выявленных проблем (последние 50 записей)")
    st.caption("Здесь отображаются закрытые инциденты. Вы можете пометить ошибочные клики как баг системы.")
    
    with get_connection() as conn:
        if include_tests:
            query = """
                SELECT 
                    a.id, a.detected_at, a.resolved_at, a.item_name, a.anomaly_type, a.qty_physical, a.source, a.comment,
                    (SELECT sku FROM stocks s WHERE s.item_name = a.item_name AND sku != '' ORDER BY report_timestamp DESC LIMIT 1) as sku
                FROM anomaly_log a
                WHERE a.status != 'Открыта' 
                  AND (
                      a.anomaly_type NOT IN ('📦 Плановый приход', 'Успешная сверка', '⏳ Догруз с сайта', '🔄 Обновление карточки')
                      OR IFNULL(a.comment, '') LIKE '%[BUG]%'
                  )
                ORDER BY a.resolved_at DESC LIMIT 50
            """
        else:
            query = """
                SELECT 
                    a.id, a.detected_at, a.resolved_at, a.item_name, a.anomaly_type, a.qty_physical, a.source, a.comment,
                    (SELECT sku FROM stocks s WHERE s.item_name = a.item_name AND sku != '' ORDER BY report_timestamp DESC LIMIT 1) as sku
                FROM anomaly_log a
                WHERE a.status != 'Открыта' 
                  AND a.anomaly_type NOT IN ('Тестовая запись', 'Системная ошибка', '📦 Плановый приход', 'Успешная сверка', '⏳ Догруз с сайта', '🔄 Обновление карточки')
                  AND IFNULL(a.comment, '') NOT LIKE '%[BUG]%'
                ORDER BY a.resolved_at DESC LIMIT 50
            """
        df_history = pd.read_sql_query(query, conn)
    
    if df_history.empty:
        st.info("В истории пока нет зафиксированных инцидентов.")
    else:
        # Отрисовываем шапку кастомной таблицы (Теперь 8 колонок)
        hc = st.columns([2, 2, 3, 2, 1, 2, 1, 1])
        for col, title in zip(hc, ["Дата", "Артикул", "Наименование", "Тип", "Факт", "Комментарий", "Баг", "Откат"]):
            col.write(f"**{title}**")
        st.divider()
        
        # Отрисовываем каждую запись как строку
        for _, row in df_history.iterrows():
            c = st.columns([2, 2, 3, 2, 1, 2, 1, 1])
            
            # 1. Дата
            c[0].caption(row['resolved_at'] or row['detected_at'])
            
            # 2. Артикул (НОВОЕ)
            sku_text = row['sku'] if pd.notna(row['sku']) and row['sku'] else "Без артикула"
            c[1].write(f"🏷️ {sku_text}")
            
            # 3. Наименование
            c[2].write(row['item_name'])
            
            is_bug = "[BUG]" in str(row['comment'])
            
            # 4. Цветовое кодирование текста
            if is_bug:
                c[3].write(f"🔴 :red[{row['anomaly_type']}]")
            elif row['anomaly_type'] == 'Успешная сверка':
                c[3].write(f":green[{row['anomaly_type']}]")
            elif row['anomaly_type'] in ['Излишек', 'Пересорт (Склад)', 'Пересорт (1С)']:
                c[3].write(f":orange[{row['anomaly_type']}]")
            elif row['anomaly_type'] in ['Утеря', 'Тихая отмена']:
                c[3].write(f":red[{row['anomaly_type']}]")
            else:
                c[3].write(row['anomaly_type'])
                
            # 5. Факт
            c[4].write(row['qty_physical'])
            
            # 6. Комментарий
            c[5].caption(str(row['comment']) if pd.notna(row['comment']) else "")
            
            # 7. Кнопка БАГ
            if is_bug:
                c[6].button("✅", key=f"bug_done_{row['id']}", disabled=True, help="Уже отмечено как баг")
            else:
                if c[6].button("🚨", key=f"mark_bug_{row['id']}", help="Пометить как сбой интерфейса/системы"):
                    with get_connection() as conn:
                        conn.execute("""
                            UPDATE anomaly_log 
                            SET comment = '[BUG] ' || IFNULL(comment, 'Ошибка классификации/UI') 
                            WHERE id = ?
                        """, (row['id'],))
                        conn.commit()
                    st.rerun()
            
            # 8. КНОПКА ВОЗВРАТА В АНОМАЛИИ
            if c[7].button("↩️", key=f"restore_hist_{row['id']}", help="Отменить решение и вернуть в Аномалии"):
                with get_connection() as conn:
                    conn.execute("DELETE FROM anomaly_log WHERE id = ?", (row['id'],))
                    conn.commit()
                if row['item_name'] in st.session_state.dismissed_names:
                    st.session_state.dismissed_names.remove(row['item_name'])
                
                st.toast(f"Товар возвращен во вкладку Аномалии!")
                st.rerun()

    st.divider()
    
    # --- НОВЫЙ БЛОК: ПОСТОЯННЫЙ ЖУРНАЛ ЛЕГАЛИЗОВАННЫХ АНОМАЛИЙ ---
    st.subheader("🙈 Журнал рутины (Легализованные аномалии)")
    st.caption("Постоянная память: здесь хранится история плановых поступлений и успешных сверок.")
    
    with get_connection() as conn:
        query_legal = """
            SELECT 
                a.id, 
                a.detected_at, 
                a.item_name, 
                a.anomaly_type, 
                a.comment,
                (a.qty_system - a.qty_physical) as delta,
                (SELECT sku FROM stocks s WHERE s.item_name = a.item_name AND sku != '' ORDER BY report_timestamp DESC LIMIT 1) as sku
            FROM anomaly_log a
            WHERE a.anomaly_type IN ('📦 Плановый приход', 'Успешная сверка', '⏳ Догруз с сайта', '🔄 Обновление карточки')
              AND IFNULL(a.comment, '') NOT LIKE '%[BUG]%'
              AND IFNULL(a.comment, '') NOT LIKE '🔗 Склеено (старое имя)%'
            ORDER BY a.detected_at DESC LIMIT 50
        """
        df_legal = pd.read_sql_query(query_legal, conn)
        
    if df_legal.empty:
        st.info("Журнал рутинных операций пока пуст.")
    else:
        st.write(f"**Последние {len(df_legal)} подтвержденных операций:**")
        
        for idx, row in df_legal.iterrows():
            # Меняем сетку на 6 колонок
            c = st.columns([2, 2, 4, 2, 1, 1])
            
            # 1. Дата
            c[0].caption(row['detected_at'][:16])
            
            # 2. Артикул
            sku_text = row['sku'] if pd.notna(row['sku']) and row['sku'] else "Без артикула"
            c[1].write(f"🏷️ {sku_text}")
            
            # 3. Название товара и комментарий (если есть)
            c[2].write(row['item_name'])
            if row['comment']:
                c[2].caption(f"💬 {row['comment']}")
                
            # 4. Статус и количество
            delta_val = int(row['delta']) if pd.notna(row['delta']) else 0
            delta_text = f"+{delta_val} шт." if delta_val > 0 else f"{delta_val} шт."
            
            if row['anomaly_type'] == 'Успешная сверка':
                c[3].write(f"🟢 :green[{row['anomaly_type']}]")
            elif row['anomaly_type'] == '⏳ Догруз с сайта':
                c[3].write(f"🟡 :orange[{row['anomaly_type']}] **{delta_text}**")
            elif row['anomaly_type'] == '🔄 Обновление карточки':
                c[3].write(f"🟣 :violet[{row['anomaly_type']}] **{delta_text}**")
            else: # 📦 Плановый приход
                c[3].write(f"⚪ :gray[{row['anomaly_type']}] **{delta_text}**")
                
            # 5. КНОПКА БАГА
            if c[4].button("🚨", key=f"leg_bug_{row['id']}", help="Ошибся кнопкой? Отправить в баги"):
                with get_connection() as conn:
                    conn.execute("""
                        UPDATE anomaly_log 
                        SET comment = '[BUG] ' || IFNULL(comment, 'Ошибочная легализация') 
                        WHERE id = ?
                    """, (row['id'],))
                    conn.commit()
                st.rerun()
            
            # --- НОВАЯ КНОПКА ВОЗВРАТА В АНОМАЛИИ ---
            if c[5].button("↩️", key=f"restore_leg_{row['id']}", help="Отменить решение и вернуть в Аномалии"):
                # 1. Удаляем из базы (Очищаем KPI)
                with get_connection() as conn:
                    conn.execute("DELETE FROM anomaly_log WHERE id = ?", (row['id'],))
                    conn.commit()
                # 2. Возвращаем на экран Аномалий
                if row['item_name'] in st.session_state.dismissed_names:
                    st.session_state.dismissed_names.remove(row['item_name'])
                
                st.toast(f"Товар возвращен во вкладку Аномалии!")
                st.rerun()
                
            st.divider() # Рисует аккуратную линию между записями

# 4. СТРАНИЦА НЕЛИКВИДОВ
elif st.session_state.current_page == "❄️ Неликвиды":
    st.subheader("❄️ Анализ замороженного капитала (Dead Stock)")
    
    df_dead = load_dead_stock_analysis()
    
    if df_dead.empty: 
        st.info("📊 Нужно больше данных. Алгоритм выявления неликвидов заработает, когда накопится история изменений.")
    else:
        only_dead = df_dead[df_dead['Заморожен']].copy()
        only_dead['Потери'] = only_dead['Цена'] * only_dead['Остаток']
        total_frozen = only_dead['Потери'].sum()
        
        # --- ВИЗУАЛ ДЛЯ МЕНЕДЖМЕНТА ---
        # Делим экран на две колонки: слева цифры, справа график
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.metric("Заморожено (Итого)", f"{total_frozen:_.0f} ₽".replace('_', ' '))
            st.caption("Товары, лежащие без движения дольше нормы (медианы) по их категории.")
            
            # Фича для бизнеса: Экспорт отчета в CSV (читается в Excel)
            csv = only_dead.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать отчет (для Закупок)",
                data=csv,
                file_name='dead_stock_report.csv',
                mime='text/csv',
                use_container_width=True
            )
            
        with c2:
            # Бизнес-логика: Группируем потери по категориям
            if not only_dead.empty:
                st.write("**Где заморожены деньги (по категориям):**")
                # Pandas группирует данные, суммирует потери и сортирует по убыванию
                category_losses = only_dead.groupby('Категория')['Потери'].sum().sort_values(ascending=False)
                # Streamlit сам рисует красивый столбчатый график
                st.bar_chart(category_losses)

        # --- ДЕТАЛЬНАЯ ТАБЛИЦА ---
        st.write("---")
        st.write("**Детализация по товарам (Топ проблемных позиций):**")
        
        # Выводим таблицу, отсортированную от самых дорогих потерь к самым дешевым
        st.dataframe(only_dead.sort_values('Потери', ascending=False), use_container_width=True, column_config={
            "Потери": st.column_config.NumberColumn(format="%d ₽"),
            "Дней без движения": st.column_config.ProgressColumn(format="%d дн.", min_value=0, max_value=365)
        })

# 5. СТРАНИЦА ОБОРАЧИВАЕМОСТИ
elif st.session_state.current_page == "📈 Оборачиваемость":
    if not st.session_state.selected_item_name: 
        st.info("👈 Перейдите во вкладку '📦 Склад', найдите нужный товар через поиск и нажмите '📈 График'.")
    else:
        target_name = st.session_state.selected_item_name
        target_sku = st.session_state.get('selected_item_sku', '')
        
        if st.button("🔙 Вернуться на склад", use_container_width=False):
            st.session_state.current_page = "📦 Склад"
            st.rerun()
            
        st.subheader(f"{target_name}")
        
        history = load_velocity_history(target_name, target_sku)
        
        if len(history) < 2: 
            st.warning("Мало данных для годового графика. Нужно накопить хотя бы 2 среза базы.")
        else:
            diff = history['Остаток'].iloc[-1] - history['Остаток'].iloc[-2]
            c1, c2 = st.columns(2)
            c1.metric("Текущий остаток", f"{int(history['Остаток'].iloc[-1])} шт.")
            c2.metric("Сдвиг (к прошлой записи)", f"{int(diff)} шт.", delta=int(diff))
            
            st.line_chart(history['Остаток'])
            
            # --- НОВЫЙ БЛОК: ИСТОРИЯ ДВИЖЕНИЯ (ЛЕДЖЕР) ---
            st.write("---")
            st.subheader("📋 Журнал движений товара")
            
            # Вычисляем разницу между днями (сравниваем текущую строку с предыдущей)
            movements = history.copy().reset_index()
            movements['Дельта'] = movements['Остаток'].diff()
            
            # Оставляем только те дни, когда остаток реально менялся
            movements = movements.dropna(subset=['Дельта'])
            movements = movements[movements['Дельта'] != 0].copy()
            
            if movements.empty:
                st.info("Движений по данному товару не зафиксировано.")
            else:
                # 1. Продуктовая логика: классификация 
                movements['Событие'] = movements['Дельта'].apply(lambda x: "📦 Приход (или излишек)" if x > 0 else "🛒 Расход (или утеря)")
                movements['Кол-во'] = movements['Дельта'].abs().astype(int)
                movements['Остаток'] = movements['Остаток'].astype(int)
                
                # 2. Дата фиксации парсером (когда скрипт увидел изменение)
                movements['Дата фиксации'] = movements['Дата'].dt.strftime('%Y-%m-%d')
                
                # 3. БИЗНЕС-ЛОГИКА (Сдвиг даты): изменение произошло ВЧЕРА днем или СЕГОДНЯ ночью
                # Отнимаем 1 день от даты фиксации
                movements['Фактическое время'] = (movements['Дата'] - pd.Timedelta(days=1)).dt.strftime('%Y-%m-%d') + " (вчера/ночь)"
                
                # 4. Формируем красивую таблицу для пользователя, сортируем от новых к старым
                display_df = movements[['Дата фиксации', 'Фактическое время', 'Событие', 'Кол-во', 'Остаток']].sort_values(by='Дата фиксации', ascending=False)
                
                st.dataframe(
                    display_df, 
                    use_container_width=True, 
                    hide_index=True
                )

elif st.session_state.current_page == "🔥 Задачи":
    df_tasks = load_anomaly_report("Открыта") #
    
    if df_tasks.empty:
        st.success("Все задачи выполнены!")
    else:
        latest_inv = load_inventory() #
        
        for idx, row in df_tasks.iterrows():
            with st.expander(f"📌 {row['item_name']} ({row['anomaly_type']})"):
                # 1. Получаем текущее значение с сайта
                current_site_qty_list = latest_inv[latest_inv['Наименование'] == row['item_name']]['Остаток'].values
                current_site_qty = int(current_site_qty_list[0]) if len(current_site_qty_list) > 0 else 0
                
                # 2. Показываем динамику процесса
                m1, m2, m3 = st.columns(3)
                m1.metric("Было в 1С (при фиксации)", f"{row['qty_system']} шт.")
                m2.metric("Твой замер (факт/оценка)", f"{row['qty_physical']} шт.")
                # Дельта показывает, сколько офис "вернул" в систему
                m3.metric("Сейчас на сайте", f"{current_site_qty} шт.", 
                          delta=int(current_site_qty - row['qty_system']))
                
                # 3. Финальное решение
                st.write("---")
                
                # Выбор причины закрытия, чтобы не портить MTTR склада
                close_reason = st.radio(
                    "Что это было?", 
                    ["Обычное расхождение (ошибка склада/1С)", "Просто лаг сайта (Догруз данных)"],
                    key=f"reason_{row['id']}"
                )
                
                final_note = st.text_input("Заметка при закрытии (опционально):", 
                                          placeholder="Напр: Данные в 1С обновлены, остаток корректен",
                                          key=f"note_{row['id']}")
                
                bc1, bc2 = st.columns(2)
                if bc1.button("✅ Вопрос решен", key=f"close_{row['id']}", type="primary", use_container_width=True):
                    
                    # Если это вина сайта, принудительно меняем тип аномалии
                    # Это исключит задачу из расчета MTTR
                    if close_reason == "Просто лаг сайта (Догруз данных)":
                        with get_connection() as conn:
                            conn.execute("UPDATE anomaly_log SET anomaly_type = '⏳ Догруз с сайта' WHERE id = ?", (row['id'],))
                            conn.commit()
                            
                    close_anomaly_in_db(row['id'], final_note)
                    st.rerun()
                
                if bc2.button("🗑️ Отменить запись", key=f"cancel_{row['id']}", use_container_width=True):
                    cancel_anomaly_in_db(row['id'], final_note) 
                    st.rerun()

elif st.session_state.current_page == "📥 Приемка":
    st.subheader("📸 Оцифровка накладной (Нейро-приемка)")
    st.caption("Загрузите фото таблицы с товарами. Цены и контрагентов в кадр брать не нужно.")
    

    from google import genai 
    from PIL import Image
    import json
    import os
    
    api_key = st.secrets["GEMINI_API_KEY"]
    
    if api_key:

        os.environ['HTTPS_PROXY'] = "socks5://127.0.0.1:1080"
        os.environ['HTTP_PROXY'] = "socks5://127.0.0.1:1080"


        client = genai.Client(api_key=api_key)
        
        # Оставляем только загрузку из галереи по твоей просьбе
        file_photo = st.file_uploader("📂 Выберите фото из галереи (накладная):", type=["jpg", "jpeg", "png"])
        
        if file_photo:
            st.image(file_photo, caption="📸 Фото загружено", width=400)
            
            if st.button("🚀 Отправить в Gemini 3.1 на оцифровку", type="primary", use_container_width=True):
                with st.spinner("🧠 Нейросеть Gemini 3.1 Flash Lite читает таблицу..."):
                    try:
                        img = Image.open(file_photo)
                        
                        # Строгий промпт
                        prompt = """
                        Ты — точный алгоритм оцифровки документов. 
                        На этой картинке таблица с товарами (накладная). 
                        
                        ТВОЯ ЗАДАЧА:
                        Извлечь данные из ячеек "Артикул", "Товары" и "Кол-во" СТРОГО 1 в 1 как напечатано на бумаге.
                        
                        ПРАВИЛА:
                        1. Название: Перепиши весь текст ячейки полностью. Обязательно сохраняй все скобки, размеры и приписки (например, "(L=53мм) - рычаг (10/100шт.)"). Ничего не сокращай!
                        2. Артикул: Перепиши всё содержимое ячейки. Если там есть название бренда (например, "Джакомини Рус") или перенос строки, склей это в одну строку и сохрани. Не обрезай текст!
                        3. Количество: Верни только цифру.
                        
                        ВЕРНИ СТРОГО МАССИВ JSON И БОЛЬШЕ НИЧЕГО. 
                        Формат:
                        [
                          {"название": "Кран шаровый латунный... (L=53мм) - рычаг...", "артикул": "R850X023 Джакомини Рус", "количество": 100}
                        ]
                        """
                        
                        # Вызов через новый SDK и модель 3.1 Flash Lite
                        response = client.models.generate_content(
                            model="gemini-3.1-flash-lite-preview",
                            contents=[prompt, img]
                        )
                        
                        # Очистка текста от маркдауна и парсинг
                        raw_text = response.text.replace("```json", "").replace("```", "").strip()
                        items_list = json.loads(raw_text)
                        
                        st.success(f"✅ Распознано позиций: {len(items_list)}")
                        st.session_state.temp_invoice = items_list
                        
                    except Exception as e:
                        st.error(f"❌ Ошибка распознавания: {e}")
                        st.info("💡 Убедитесь, что Bitvise SSH (прокси) залогинен и работает на ноутбуке.")
            
            # Блок сохранения результата
            if 'temp_invoice' in st.session_state:
                st.write("---")
                st.write("**Результат оцифровки:**")
                df_result = pd.DataFrame(st.session_state.temp_invoice)
                
                st.dataframe(df_result, use_container_width=True, hide_index=True)
                
                if st.button("💾 Подтвердить и сохранить в Ожидаемые приходы", type="primary"):
                    with get_connection() as conn:
                        for item in st.session_state.temp_invoice:
                            try:
                                qty = int(item.get('количество', 0))
                            except (ValueError, TypeError):
                                qty = 0
                                
                            conn.execute("""
                                INSERT INTO expected_deliveries (item_name, sku, qty_expected) 
                                VALUES (?, ?, ?)
                            """, (str(item.get('название', '')), str(item.get('артикул', '')), qty))
                        conn.commit()
                    
                    del st.session_state.temp_invoice
                    st.success("🎉 Данные успешно добавлены в список ожидания!")
                    st.rerun()
            
    st.divider()
    st.subheader("📋 Список ожидаемых товаров")
    st.caption("Эти позиции были оцифрованы и ждут появления на сайте для авто-легализации аномалий.")
    
    with get_connection() as conn:
        # Вытаскиваем только те товары, которые еще не были легализованы
        expected_df = pd.read_sql_query(
            "SELECT id, created_at, sku, item_name, qty_expected FROM expected_deliveries WHERE status = 'Ожидает' ORDER BY created_at DESC", 
            conn
        )
        
    if expected_df.empty:
        st.info("В листе ожидания пока ничего нет.")
    else:
        # Рисуем шапку таблицы
        hc = st.columns([2, 2, 4, 2, 1])
        for col, title in zip(hc, ["Дата сканирования", "Артикул", "Наименование", "Ожидаем", "Действие"]):
            col.write(f"**{title}**")
        st.divider()
        
        # Построчный вывод каждого ожидаемого товара
        for _, row in expected_df.iterrows():
            c = st.columns([2, 2, 4, 2, 1])
            
            # 1. Дата (обрезаем до минут для красоты)
            c[0].caption(str(row['created_at'])[:16])
            
            # 2. Артикул
            sku_text = row['sku'] if pd.notna(row['sku']) and row['sku'] else "—"
            c[1].write(sku_text)
            
            # 3. Название
            c[2].write(row['item_name'])
            
            # 4. Количество
            c[3].write(f"{row['qty_expected']} шт.")
            
            # 5. Кнопка удаления (полностью удаляет строку из БД)
            if c[4].button("❌", key=f"del_exp_{row['id']}", help="Удалить позицию из листа ожидания"):
                with get_connection() as conn:
                    conn.execute("DELETE FROM expected_deliveries WHERE id = ?", (row['id'],))
                    conn.commit()
                st.toast(f"🗑️ Товар удален из ожидания: {row['item_name']}")
                st.rerun()
            
            st.divider()

elif st.session_state.current_page == "⚖️ A/B Тест: AI vs Человек":
    st.subheader("⚖️ A/B Тест: AI-прогноз vs Человеческие решения")
    st.caption("Теневой режим работы: алгоритм делает прогнозы закупок и сверяет их с реальными действиями менеджеров. Это позволяет оценить упущенную выгоду без вмешательства в текущие бизнес-процессы.")
    
    # --- ИНДИКАТОР ПРОГРЕВА МОДЕЛИ (COLD START) ---
    with get_connection() as conn:
        # Считаем, сколько дней истории у нас есть
        days_in_db_query = "SELECT COUNT(DISTINCT SUBSTR(report_timestamp, 1, 10)) FROM stocks"
        days_in_db = conn.execute(days_in_db_query).fetchone()[0]
        
    if days_in_db < 30:
        st.warning(f"⚠️ **Модель в стадии 'прогрева' (Cold Start):** Накоплено данных за {days_in_db} из 30 необходимых дней. До завершения сбора полной базы, ИИ экстраполирует короткие тренды, что может приводить к повышенной погрешности (ложным срабатываниям Перезатарки).")
    else:
        st.success(f"✅ **Модель обучена:** Накоплено данных за {days_in_db} дней. Точность прогнозов оптимальна.")

    # 1. Запускаем фоновую проверку прогнозов при входе на вкладку
    verify_shadow_forecasts()
    
    with get_connection() as conn:
        # Подтягиваем прогнозы + актуальный остаток прямо из таблицы stocks
        df_forecasts = pd.read_sql_query("""
            SELECT 
                f.*,
                (SELECT quantity FROM stocks s WHERE s.item_name = f.item_name ORDER BY report_timestamp DESC LIMIT 1) as current_qty
            FROM ai_forecasts f 
            ORDER BY f.created_at DESC
        """, conn)

    if df_forecasts.empty:
        st.info("Пока нет активных прогнозов. Сгенерируйте их с помощью кнопки ниже.")
    else:
        # 2. Считаем продуктовые метрики (Shadow ROI)
        total_lost = df_forecasts['lost_sales_value'].sum()
        total_overstock = df_forecasts['overstock_value'].sum()
        
        m1, m2 = st.columns(2)
        m1.metric("📉 Упущенная выгода (Prevented Lost Sales)", f"{total_lost:,.0f} ₽".replace(',', ' '), help="Сколько компания потеряла из-за того, что товар кончился, а закупка не была сделана вовремя.")
        m2.metric("🧊 Замороженный капитал за последние 30 дней (Cost of Overstock)", f"{total_overstock:,.0f} ₽".replace(',', ' '), help="Сумма излишков, купленных сверх рекомендаций ИИ.")
        
        st.write("---")
        st.write("**Детализация (Журнал прогнозов и финансовых последствий):**")
        
        # Берем нужные колонки (добавлен current_qty)
        display_df = df_forecasts[['created_at', 'item_name', 'current_qty', 'predicted_zero_date', 'recommended_qty', 'reason', 'status', 'lost_sales_value', 'overstock_value']].copy()
        
        # Делаем остаток красивым целым числом
        display_df['current_qty'] = display_df['current_qty'].fillna(0).astype(int)
        
        display_df['Упущенная выручка (₽)'] = display_df['lost_sales_value'].apply(lambda x: f"{x:,.0f} ₽".replace(',', ' ') if x > 0 else "")
        display_df['Заморожено (₽)'] = display_df['overstock_value'].apply(lambda x: f"{x:,.0f} ₽".replace(',', ' ') if x > 0 else "")
        
        display_df.rename(columns={
            'created_at': 'Дата прогноза',
            'item_name': 'Товар',
            'current_qty': 'Остаток (шт)',  # <--- ВОТ ЭТА НОВАЯ СТРОЧКА
            'predicted_zero_date': 'ИИ: Обнулится',
            'recommended_qty': 'ИИ: Заказать (шт)',
            'reason': 'Обоснование',
            'status': 'Статус / Результат'
        }, inplace=True)
        
        # Отрезаем секунды у даты
        display_df['Дата прогноза'] = display_df['Дата прогноза'].str[:10]
        
        # Отрисовываем таблицу, убрав сырые технические колонки с нулями
        st.dataframe(
            display_df.drop(columns=['lost_sales_value', 'overstock_value']), 
            use_container_width=True, 
            hide_index=True
        )

    st.divider()
    
    pending_flag = Path("logs/ai_pending.flag")
    
    # 1. Считаем прогнозы за сегодня для понимания статуса
    with sqlite3.connect(DB_PATH) as conn:
         forecasts_today = conn.execute("SELECT COUNT(*) FROM ai_forecasts WHERE date(created_at) = date('now', 'localtime')").fetchone()[0]

    # 2. Информационные уведомления
    if pending_flag.exists():
        st.warning("⚠️ **Есть необработанные данные:** Парсер собрал свежую информацию, но ИИ-анализ ещё не запущен. Нажмите кнопку ниже.")
        btn_text = "🚀 Запустить анализ свежих данных"
        btn_type = "primary"
    elif forecasts_today > 0:
        st.info(f"✅ **План на сегодня выполнен.** В базе уже есть {forecasts_today} прогнозов за текущие сутки.")
        btn_text = "🔄 Принудительный пересчет"
        btn_type = "secondary"
    else:
        btn_text = "🚀 Запустить первичный анализ"
        btn_type = "primary"

    # 3. Кнопка запуска (Всегда активна, чтобы пользователь мог сам проверить связь)
    if st.button(btn_text, type=btn_type, use_container_width=True):
        with st.spinner("🤖 ИИ анализирует графики продаж..."):
            try:
                from ai_forecaster import run_batch_forecast
                status = run_batch_forecast()
                
                if status == "no_key":
                    st.error("❌ Не найден API ключ Gemini!")
                elif status == "empty":
                    st.warning("⚠️ Не найдено товаров для анализа.")
                    if pending_flag.exists(): pending_flag.unlink()
                elif status and status.startswith("error_"):
                    # Вот здесь пользователь увидит реальную ошибку прокси/связи, если она есть
                    err_text = status.split('_', 1)[1]
                    st.error(f"❌ Ошибка связи с ИИ: {err_text}")
                elif status and status.startswith("ok_"):
                    count = status.split('_')[1]
                    st.success(f"✅ Готово! Сгенерировано прогнозов: {count}.")
                    if pending_flag.exists(): pending_flag.unlink()
                         
            except Exception as e:
                st.error(f"❌ Критическая ошибка: {e}")