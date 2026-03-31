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
            df['_search_index'] = (df['Наименование'].fillna('') + ' ' + df['Артикул'].fillna('')).str.lower().str.replace('ё', 'е')
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
    st.title("💎 Stock Shadow")
    menu_options = ["📦 Склад", f"⚠️ Аномалии ({active_anom_count})", f"🔥 Задачи ({open_tasks_count})", f"✅ Архив ({len(st.session_state.dismissed_names)})", "❄️ Неликвиды", "📈 Оборачиваемость"]
    
    try: 
        current_idx = [m.split(' (')[0] for m in menu_options].index(st.session_state.current_page.split(' (')[0])
    except ValueError: 
        current_idx = 0
    
    choice = st.radio("Меню", menu_options, index=current_idx)
    st.session_state.current_page = choice.split(' (')[0]

    st.write("---")
    if db_stats:
        st.caption("📊 Статистика базы")
        st.info(f"Начало: {db_stats['start']}\n\nДней в базе: {db_stats['days_count']}")
    
    if st.button("🔄 Обновить данные", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    if st.button("🗑️ Очистить архив", use_container_width=True):
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
        
        if 0 < len(f_df) <= 20:
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
            st.dataframe(f_df.drop(columns=['ID']), use_container_width=True, height=500)
    else: 
        st.dataframe(df_inv.drop(columns=['_search_index', 'ID']), use_container_width=True, height=500)

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
                btn_cols = st.columns(5)
                reasons = [
                    ("Тихая отмена", "отмена"), 
                    ("Пересорт (Склад)", "склад"), 
                    ("Пересорт (1С)", "офис"), 
                    ("Излишек", "плюс"), 
                    ("Утеря", "минус")
                ]

                for i, (label, key_suffix) in enumerate(reasons):
                    if btn_cols[i].button(label, key=f"anom_{idx}_{key_suffix}", use_container_width=True):
                        price = df_inv[df_inv['Наименование'] == row['Наименование']]['Цена'].values[0] if not df_inv.empty else 0
                        
                        anomaly_data = {
                            "item_name": row['Наименование'],
                            "anomaly_type": label,
                            "qty_system": row['Стало'],
                            "qty_physical": row['Было'], 
                            "financial_impact": abs(row['Дельта'] * price),
                            "source": "Автоматически",
                            "status": "Открыта",
                            "comment": ""
                        }
                        save_anomaly_to_db(anomaly_data)
                        st.success(f"Зафиксировано: {label}")
                        st.rerun()
            st.divider()

# 3. СТРАНИЦА АРХИВА И KPI
elif st.session_state.current_page == "✅ Архив":
    
    # 🧪 ПЕРЕКЛЮЧАТЕЛЬ DEV MODE
    hc1, hc2 = st.columns([3, 1])
    hc1.subheader("🎯 KPI: Эффективность и Качество (Lean Model)")
    include_tests = hc2.checkbox("🧪 Тестовые данные", value=False, help="Показать тестовые записи для отладки")
    
    with get_connection() as conn:
        if include_tests:
            df_kpi = pd.read_sql_query("SELECT source, anomaly_type FROM anomaly_log", conn)
        else:
            df_kpi = pd.read_sql_query("SELECT source, anomaly_type FROM anomaly_log WHERE anomaly_type != 'Тестовая запись'", conn)
        
    if df_kpi.empty:
        st.info("Пока нет данных для расчета KPI.")
    else:
        # --- 1. ПАРАМЕТРЫ РУТИНЫ (Шаги) ---
        OVERHEAD_MINUTES = 20  
        MEDIAN_BATCH_SIZE = 17 
        min_per_item = OVERHEAD_MINUTES / MEDIAN_BATCH_SIZE
        
	# 2. ПАРАМЕТРЫ КОММУНИКАЦИЙ (Activity-Based Costing)
        # Сколько минут сжигает одна проблема, если её найдет клиент, а не мы?
        TIME_WAREHOUSE_WAITING = 10 # мин. простоя кладовщика (дозвон, ожидание ответа, переупаковка)
        TIME_OFFICE_INVESTIGATION = 10 # мин. работы офиса (поиск в 1С, исправление документов)
        
        # Общая стоимость одной эскалации в человеко-минутах
        ESCALATION_COST_MIN = TIME_WAREHOUSE_WAITING + TIME_OFFICE_INVESTIGATION
        
        # --- РАСЧЕТЫ ---
        total_checks = len(df_kpi)
        
        # Экономия 1: Ногами (все проверки)
        routine_saved_hours = (total_checks * min_per_item) / 60
        
        # Считаем только РЕАЛЬНЫЕ проблемы, найденные ДО прихода клиента
        # (Успешная сверка - это не проблема, мы её исключаем из этого расчета)
        proactive_issues = len(df_kpi[
            (df_kpi['source'].isin(['Автоматически', 'Вручную (План)'])) & 
            (df_kpi['anomaly_type'] != 'Успешная сверка')
        ])
        
        # Экономия 2: Головой и языком (предотвращенные звонки)
        communication_saved_hours = (proactive_issues * ESCALATION_COST_MIN) / 60
        
        # ИТОГО
        total_saved_hours = routine_saved_hours + communication_saved_hours
        
        # Расчет Proactive Rate
        proactive_count = len(df_kpi[df_kpi['source'].isin(['Автоматически', 'Вручную (План)'])])
        proactive_rate = (proactive_count / total_checks) * 100 if total_checks > 0 else 0
        
        # Отрисовка дашборда
        kc1, kc2, kc3 = st.columns(3)
        kc1.metric("Сэкономлено времени (Итого)", f"{total_saved_hours:.1f} ч.", 
                  help=f"Рутина (ноги): {routine_saved_hours:.1f}ч + Коммуникации (звонки): {communication_saved_hours:.1f}ч")
        kc2.metric("Предотвращено эскалаций", proactive_issues, 
                  help=f"Ошибки, найденные проактивно. Каждая сэкономила ~{ESCALATION_COST_MIN} мин. разборок с офисом")
        kc3.metric("Proactive Rate", f"{proactive_rate:.1f}%", delta="Цель: 100%")
        st.progress(proactive_rate / 100.0)

    st.divider()
    st.subheader("📜 История выявленных проблем")
    
    with get_connection() as conn:
        # Формируем SQL-запрос для истории в зависимости от галочки
        if include_tests:
            query = """
                SELECT detected_at, resolved_at, item_name, anomaly_type, qty_physical, source, comment 
                FROM anomaly_log 
                WHERE status != 'Открыта' AND anomaly_type != 'Успешная сверка' 
                ORDER BY resolved_at DESC
            """
        else:
            query = """
                SELECT detected_at, resolved_at, item_name, anomaly_type, qty_physical, source, comment 
                FROM anomaly_log 
                WHERE status != 'Открыта' AND anomaly_type NOT IN ('Успешная сверка', 'Тестовая запись')
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
                if bc1.button("✅ Вопрос решен (в Архив)", key=f"close_{row['id']}", use_container_width=True):
                    # Используем готовую функцию-обертку, которую мы создали ранее!
                    close_anomaly_in_db(row['id'], final_note)
                    st.rerun()
                
                if bc2.button("🗑️ Отменить запись", key=f"cancel_{row['id']}", use_container_width=True):
                     # Передаем текст из поля final_note при отмене
                    cancel_anomaly_in_db(row['id'], final_note) 
                    st.rerun()
                
                st.caption(f"📅 Дата: {row['detected_at']} | Тип: {row['anomaly_type']}")