import sys
import streamlit as st
import pandas as pd
from pathlib import Path
import sqlite3
import db
import ai_services
from views import dead_stock_view, efficiency_view, anomalies_view, velocity_view
from contextlib import contextmanager

sys.path.insert(0, str(Path(__file__).resolve().parent))
from queries import get_anomalies_query, get_insert_anomaly_query, get_close_anomaly_query, get_cancel_anomaly_query, get_sla_metrics_query

import math


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
    with db.get_connection() as conn:
        # 1. Теперь берем ВСЕ статусы, кроме финальных (Упущенная выгода и Точный прогноз)
        forecasts = pd.read_sql_query("""
            SELECT * FROM ai_forecasts 
            WHERE status NOT IN ('📉 Упущенная выгода', '✅ Точный прогноз', '🔄 Пересчитан ИИ')
        """, conn)
        
        if forecasts.empty: return
        
        latest_inv = db.load_inventory()
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



st.set_page_config(page_title="Stock Shadow | Analytics", page_icon="💎", layout="wide")

# Скрываем лишнее
st.markdown("<style>#MainMenu {visibility: hidden;} footer {visibility: hidden;}</style>", unsafe_allow_html=True)

# --- ИНИЦИАЛИЗАЦИЯ ПАМЯТИ ---
if 'dismissed_names' not in st.session_state:
    st.session_state.dismissed_names = []
    if db.DB_PATH.exists():
        try:
            with sqlite3.connect(db.DB_PATH) as conn:
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

# --- ЛОГИКА НАВИГАЦИИ ---
df_inv = db.load_inventory()
df_anomalies = db.load_anomalies()
db_stats = db.get_db_stats()

# Фильтруем активные аномалии по именам
active_anom_count = len(df_anomalies[~df_anomalies['Наименование'].isin(st.session_state.dismissed_names)]) if not df_anomalies.empty else 0

# Безопасно считаем открытые задачи из базы
try:
    with db.get_connection() as conn:
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
    with db.get_connection() as conn:
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
        is_proxy_ok = ai_services.check_ai_connection()
        if not is_proxy_ok:
            st.error("🚨 **Системное предупреждение:** Парсер собрал новые данные, но ИИ-прогнозы не построены (нет связи с OpenRouter API).")
        else:
            st.warning("⚠️ **ИИ ожидает запуска:** В системе есть свежие не проанализированные данные. Перейдите на вкладку '⚖️ A/B Тест' и нажмите кнопку запуска.")

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
                    db.save_anomaly_to_db({
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
                        
                        db.save_anomaly_to_db({
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
        
        with db.get_connection() as conn:
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

            # --- НОВЫЙ БЛОК: ИСЧЕЗНУВШИЕ ТОВАРЫ (С ИНТЕРАКТИВОМ) ---
            if len(df_stats) > 1:
                yesterday_date = df_stats.iloc[1]['parse_date']
                
                # Ищем товары, которые парсер видел вчера, но не увидел сегодня
                lost_items = df_inv[(df_inv['last_seen_date'] == yesterday_date) & (~df_inv['actual'])].copy()
                
                # Убираем те, которые мы уже обработали (кликнули кнопки)
                lost_items = lost_items[~lost_items['Наименование'].isin(st.session_state.dismissed_names)]
                
                if not lost_items.empty:
                    with st.expander(f"📉 Сняты с сайта (Требует проверки: {len(lost_items)} шт.)", expanded=True):
                        st.warning("👀 **Слепая зона:** Эти товары исчезли с сайта. Подтвердите физическое наличие на полке.")
                        
                        for idx, row in lost_items.iterrows():
                            c = st.columns([2, 4, 2, 3])
                            c[0].write(f"🏷️ {row['Артикул']}")
                            c[1].write(row['Наименование'])
                            c[2].write(f"Было: **{row['Остаток']} шт.**")
                            
                            btn_col1, btn_col2 = c[3].columns(2)
                            
                            # КНОПКА 1: Легальная продажа (Убираем из списка без записи в аномалии)
                            if btn_col1.button("🛒 Продан", key=f"lost_sold_{row['ID']}", help="Товара реально больше нет на полке", use_container_width=True):
                                st.session_state.dismissed_names.append(row['Наименование'])
                                st.rerun()
                                
                            # КНОПКА 2: Ошибка витрины (Баг - пишем в KPI)
                            if btn_col2.button("🚨 Баг 1С", key=f"lost_bug_{row['ID']}", help="Товар лежит на полке, но сайт его скрыл!", type="primary", use_container_width=True):
                                db.save_anomaly_to_db({
                                    "item_name": row['Наименование'],
                                    "anomaly_type": "Скрыт с витрины (Баг)",
                                    "qty_system": 0, # На сайте 0 (его нет)
                                    "qty_physical": row['Остаток'], # По факту он есть
                                    "financial_impact": row['Остаток'] * row['Цена'], # Упущенная выгода!
                                    "source": "Автоматически",
                                    "status": "Закрыта", # Закрываем сразу, чтобы не висел в задачах
                                    "comment": "Товар физически на складе, но исчез с сайта (Упущенная выручка)"
                                })
                                st.session_state.dismissed_names.append(row['Наименование'])
                                st.toast("✅ Инцидент 'Упущенная выручка' записан в KPI!")
                                st.rerun()
                            st.divider()
                else:
                    st.success("✅ С момента прошлого парсинга ни один товар не пропал с сайта, либо все пропажи уже проверены.")

# 2. СТРАНИЦА АНОМАЛИЙ
elif st.session_state.current_page == "⚠️ Аномалии":
    anomalies_view.show(df_anomalies, df_inv)
# 3. СТРАНИЦА ЭФФЕКТИВНОСТИ И KPI (бывший Архив)
elif st.session_state.current_page == "🎯 Эффективность":
    efficiency_view.show()

# 4. СТРАНИЦА НЕЛИКВИДОВ
elif st.session_state.current_page == "❄️ Неликвиды":
    dead_stock_view.show()

# 5. СТРАНИЦА ОБОРАЧИВАЕМОСТИ
elif st.session_state.current_page == "📈 Оборачиваемость":
    velocity_view.show()

elif st.session_state.current_page == "🔥 Задачи":
    df_tasks = db.load_anomaly_report("Открыта") #
    
    if df_tasks.empty:
        st.success("Все задачи выполнены!")
    else:
        latest_inv = db.load_inventory() #
        
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
                        with db.get_connection() as conn:
                            conn.execute("UPDATE anomaly_log SET anomaly_type = '⏳ Догруз с сайта' WHERE id = ?", (row['id'],))
                            conn.commit()
                            
                    db.close_anomaly_in_db(row['id'], final_note)
                    st.rerun()
                
                if bc2.button("🗑️ Отменить запись", key=f"cancel_{row['id']}", use_container_width=True):
                    db.cancel_anomaly_in_db(row['id'], final_note) 
                    st.rerun()

elif st.session_state.current_page == "📥 Приемка":
    st.subheader("📸 Оцифровка накладной (Нейро-приемка)")
    st.caption("Загрузите фото таблицы с товарами. Цены и контрагентов в кадр брать не нужно.")
        
    # Оставляем только загрузку из галереи по твоей просьбе
    file_photo = st.file_uploader("📂 Выберите фото из галереи (накладная):", type=["jpg", "jpeg", "png"])
    
    if file_photo:
        st.image(file_photo, caption="📸 Фото загружено", width=400)
        
        if st.button("🚀 Отправить на оцифровку", type="primary", use_container_width=True):
            with st.spinner("🧠 Нейросеть Gemini читает таблицу..."):
                try:
                    items_list = ai_services.digitize_invoice(file_photo)
                    
                    st.success(f"✅ Распознано позиций: {len(items_list)}")
                    st.session_state.temp_invoice = items_list
                    
                except Exception as e:
                    st.error(f"❌ Ошибка распознавания: {e}")
                    
        
        # Блок сохранения результата
        if 'temp_invoice' in st.session_state:
            st.write("---")
            st.write("**Результат оцифровки:**")
            df_result = pd.DataFrame(st.session_state.temp_invoice)
            
            st.dataframe(df_result, use_container_width=True, hide_index=True)
            
            if st.button("💾 Подтвердить и сохранить в Ожидаемые приходы", type="primary"):
                with db.get_connection() as conn:
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
    
    with db.get_connection() as conn:
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
                with db.get_connection() as conn:
                    conn.execute("DELETE FROM expected_deliveries WHERE id = ?", (row['id'],))
                    conn.commit()
                st.toast(f"🗑️ Товар удален из ожидания: {row['item_name']}")
                st.rerun()
            
            st.divider()

elif st.session_state.current_page == "⚖️ A/B Тест: AI vs Человек":
    st.subheader("⚖️ A/B Тест: AI-прогноз vs Человеческие решения")
    st.caption("Теневой режим работы: алгоритм делает прогнозы закупок и сверяет их с реальными действиями менеджеров. Это позволяет оценить упущенную выгоду без вмешательства в текущие бизнес-процессы.")
    
    # --- ИНДИКАТОР ПРОГРЕВА МОДЕЛИ (COLD START) ---
    with db.get_connection() as conn:
        # Считаем, сколько дней истории у нас есть
        days_in_db_query = "SELECT COUNT(DISTINCT SUBSTR(report_timestamp, 1, 10)) FROM stocks"
        days_in_db = conn.execute(days_in_db_query).fetchone()[0]
        
    if days_in_db < 30:
        st.warning(f"⚠️ **Модель в стадии 'прогрева' (Cold Start):** Накоплено данных за {days_in_db} из 30 необходимых дней. До завершения сбора полной базы, ИИ экстраполирует короткие тренды, что может приводить к повышенной погрешности (ложным срабатываниям Перезатарки).")
    else:
        st.success(f"✅ **Модель обучена:** Накоплено данных за {days_in_db} дней. Точность прогнозов оптимальна.")

    # 1. Запускаем фоновую проверку прогнозов при входе на вкладку
    verify_shadow_forecasts()
    
    with db.get_connection() as conn:
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
    with sqlite3.connect(db.DB_PATH) as conn:
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
                status = ai_services.run_batch_forecast()
                
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