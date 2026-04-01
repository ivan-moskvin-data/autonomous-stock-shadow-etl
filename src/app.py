import sys
import streamlit as st
import pandas as pd
from pathlib import Path
import sqlite3
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parent))
from queries import get_anomalies_query, get_insert_anomaly_query, get_close_anomaly_query, get_cancel_anomaly_query

# --- НАСТРОЙКИ ПУТЕЙ ---
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"

st.set_page_config(page_title="Stock Shadow | Analytics", page_icon="💎", layout="wide")

# Скрываем лишнее
st.markdown("<style>#MainMenu {visibility: hidden;} footer {visibility: hidden;}</style>", unsafe_allow_html=True)

# --- ИНИЦИАЛИЗАЦИЯ ПАМЯТИ ---
if 'dismissed_names' not in st.session_state:
    st.session_state.dismissed_names = []
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
        if not latest_date: return pd.DataFrame()
        
        query = """
            SELECT id as 'ID', sku as 'Артикул', item_name as 'Наименование', price as 'Цена', quantity as 'Остаток', category as 'Категория' 
            FROM stocks 
            WHERE SUBSTR(report_timestamp, 1, 10) = :latest
        """
        df = pd.read_sql_query(query, conn, params={"latest": latest_date})
        
        if not df.empty:
            # Теперь поиск будет искать и по Названию, и по Артикулу, и по Категории
            df['_search_index'] = (
                df['Наименование'].fillna('') + ' ' + 
                df['Артикул'].fillna('') + ' ' + 
                df['Категория'].fillna('') # <--- Добавили категорию в "мозги" поиска
            ).str.lower().str.replace('ё', 'е')
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
def load_velocity_history(item_name: str) -> pd.DataFrame:
    if not DB_PATH.exists() or not item_name: return pd.DataFrame()
    with get_connection() as conn:
        query = "SELECT SUBSTR(report_timestamp, 1, 10) as 'Дата', quantity as 'Остаток' FROM stocks WHERE item_name = :item_name AND report_timestamp >= date('now', '-365 days') ORDER BY report_timestamp ASC"
        df = pd.read_sql_query(query, conn, params={"item_name": item_name})
        
    if not df.empty:
        df = df.drop_duplicates(subset=['Дата'], keep='last')
        df['Дата'] = pd.to_datetime(df['Дата'])
        df.set_index('Дата', inplace=True)
    return df

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
    op_options = ["📦 Склад", f"⚠️ Аномалии ({active_anom_count})", f"🔥 Задачи ({open_tasks_count})"]
    
    op_idx = next((i for i, opt in enumerate(op_options) if opt.startswith(base_page)), None)
    st.radio("Рабочая область", op_options, index=op_idx, key="op_nav", on_change=nav_changed, args=("op",))
    
    st.write("---")
    
    # --- ЛОГИЧЕСКОЕ РАЗДЕЛЕНИЕ МЕНЮ: АНАЛИТИКА ---
    st.caption("📊 АНАЛИТИКА И KPI")
    ana_options = ["🎯 Эффективность", "❄️ Неликвиды", "📈 Оборачиваемость"]
    
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
                c[0].write(row['Артикул'])
                c[1].write(row['Наименование'])
                c[2].write(f"{row['Цена']:.0f} ₽")
                c[3].write(f"{row['Остаток']} шт.")
                
                # ТРИ КНОПКИ В КОЛОНКЕ (📈 График, ⚠️ Ошибка, ✅ Всё ок)
                btn_c = c[4].columns(3)
                
                if btn_c[0].button("📈", key=f"v_{row['ID']}", help="График оборачиваемости"):
                    st.session_state.selected_item_name = row['Наименование']
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
        # Убираем технические поля, Категорию и скрываем индекс
        st.dataframe(df_inv.drop(columns=['_search_index', 'ID', 'Категория']), use_container_width=True, height=500, hide_index=True)

# 2. СТРАНИЦА АНОМАЛИЙ
elif st.session_state.current_page == "⚠️ Аномалии":
    active_anom = df_anomalies[~df_anomalies['Наименование'].isin(st.session_state.dismissed_names)] if not df_anomalies.empty else pd.DataFrame()
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

                # Ряд кнопок быстрой классификации
                # Увеличили до 6 колонок для новой кнопки
                btn_cols = st.columns(6)
                reasons = [
                    ("Тихая отмена", "отмена"), 
                    ("Пересорт (Склад)", "склад"), 
                    ("Пересорт (1С)", "офис"), 
                    ("Излишек", "плюс"), 
                    ("Утеря", "минус"),
                    ("Системная ошибка", "sys_err") # НОВАЯ КНОПКА
                ]

                for i, (label, key_suffix) in enumerate(reasons):
                    if btn_cols[i].button(label, key=f"anom_{idx}_{key_suffix}", use_container_width=True):
                        price = df_inv[df_inv['Наименование'] == row['Наименование']]['Цена'].values[0] if not df_inv.empty else 0
                        
                        # НОВАЯ ЛОГИКА: Если это системная ошибка, сразу закрываем её
                        # Чтобы она не висела в "Задачах" и не требовала проверки
                        final_status = "Закрыта" if label == "Системная ошибка" else "Открыта"
                        
                        anomaly_data = {
                            "item_name": row['Наименование'],
                            "anomaly_type": label,
                            "qty_system": row['Стало'],
                            "qty_physical": row['Было'], 
                            "financial_impact": abs(row['Дельта'] * price) if label != "Системная ошибка" else 0,
                            "source": "Автоматически",
                            "status": final_status, # ПРИМЕНЯЕМ СТАТУС
                            "comment": "Автоматическое закрытие: системный сбой данных сайта" if label == "Системная ошибка" else ""
                        }
                        save_anomaly_to_db(anomaly_data)
                        st.success(f"Зафиксировано: {label}")
                        st.rerun()
            st.divider()

# 3. СТРАНИЦА ЭФФЕКТИВНОСТИ И KPI (бывший Архив)
elif st.session_state.current_page == "🎯 Эффективность":
    
    # 🧪 ПЕРЕКЛЮЧАТЕЛЬ DEV MODE
    hc1, hc2 = st.columns([3, 1])
    hc1.subheader("🎯 KPI: Эффективность и Качество (Lean Model)")
    include_tests = hc2.checkbox("🧪 Тестовые данные", value=False, help="Показать тестовые записи для отладки")
    
    with get_connection() as conn:
        # Добавляем status, detected_at и resolved_at для расчета MTTR
        query = """
            SELECT item_name, source, anomaly_type, status, detected_at, resolved_at 
            FROM anomaly_log 
            WHERE anomaly_type NOT IN ('Тестовая запись', 'Системная ошибка')
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
        S_MTTR_NORM = 4.0 
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
    st.subheader("📜 История выявленных проблем")
    
    with get_connection() as conn:
        # Формируем SQL-запрос для истории в зависимости от галочки
        if include_tests:
            # В режиме теста видим всё (включая системные ошибки и тесты)
            query = """
                SELECT detected_at, resolved_at, item_name, anomaly_type, qty_physical, source, comment 
                FROM anomaly_log 
                WHERE status != 'Открыта' AND anomaly_type != 'Успешная сверка' 
                ORDER BY resolved_at DESC
            """
        else:
            # В рабочем режиме скрываем мусор: тесты, системные баги и рутинные сверки
            query = """
                SELECT detected_at, resolved_at, item_name, anomaly_type, qty_physical, source, comment 
                FROM anomaly_log 
                WHERE status != 'Открыта' AND anomaly_type NOT IN ('Успешная сверка', 'Тестовая запись', 'Системная ошибка')
                ORDER BY resolved_at DESC
            """
        df_history = pd.read_sql_query(query, conn)
    
    if df_history.empty:
        st.info("В истории пока нет зафиксированных инцидентов.")
    else:
        st.dataframe(df_history.rename(columns={
            'detected_at': 'Обнаружено', 'resolved_at': 'Решено', 
            'item_name': 'Товар', 'anomaly_type': 'Тип', 
            'qty_physical': 'Факт', 'source': 'Источник', 'comment': 'Заметка'
        }), use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🙈 Легализованные аномалии (текущая сессия)")
    if st.session_state.dismissed_names:
        archived = df_anomalies[df_anomalies['Наименование'].isin(st.session_state.dismissed_names)].copy()
        for idx, row in archived.iterrows():
            c = st.columns(COL_RATIOS)
            c[1].write(row['Наименование'])
            c[4].write(f":gray[+{row['Дельта']}]")
            if c[5].button("Вернуть", key=f"rev_{idx}"):
                st.session_state.dismissed_names.remove(row['Наименование'])
                st.rerun()

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
        
        if st.button("🔙 Вернуться на склад", use_container_width=False):
            st.session_state.current_page = "📦 Склад"
            st.rerun()
            
        st.subheader(f"{target_name}")
        
        history = load_velocity_history(target_name)
        
        if len(history) < 2: 
            st.warning("Мало данных для годового графика. Нужно накопить хотя бы 2 среза базы.")
        else:
            diff = history['Остаток'].iloc[-1] - history['Остаток'].iloc[-2]
            c1, c2 = st.columns(2)
            c1.metric("Текущий остаток", f"{int(history['Остаток'].iloc[-1])} шт.")
            c2.metric("Сдвиг (к прошлой записи)", f"{int(diff)} шт.", delta=int(diff))
            
            st.line_chart(history['Остаток'])

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
                final_note = st.text_input("Заметка при закрытии (опционально):", 
                                          placeholder="Напр: Данные в 1С обновлены, остаток корректен",
                                          key=f"note_{row['id']}")
                
                bc1, bc2 = st.columns(2)
                if bc1.button("✅ Вопрос решен (в 🎯 Эффективность)", key=f"close_{row['id']}", use_container_width=True):
                    # Используем готовую функцию-обертку, которую мы создали ранее!
                    close_anomaly_in_db(row['id'], final_note)
                    st.rerun()
                
                if bc2.button("🗑️ Отменить запись", key=f"cancel_{row['id']}", use_container_width=True):
                     # Передаем текст из поля final_note при отмене
                    cancel_anomaly_in_db(row['id'], final_note) 
                    st.rerun()
                
                st.caption(f"📅 Дата: {row['detected_at']} | Тип: {row['anomaly_type']}")