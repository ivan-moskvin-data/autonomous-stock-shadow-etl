import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
import db
from queries import get_sla_metrics_query
import ai_services


def show():
    """Логика вкладки '🎯 Эффективность'"""
    
    # 🧪 ПЕРЕКЛЮЧАТЕЛЬ DEV MODE
    hc1, hc2 = st.columns([3, 1])
    hc1.subheader("🎯 KPI: Эффективность и Качество (Lean Model)")
    include_tests = hc2.checkbox("🧪 Тестовые данные и баги", value=False, help="Показать тестовые записи для отладки")
    
    with db.get_connection() as conn:
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
        df_dead = db.load_dead_stock_analysis()
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

        # --- 6. РАСЧЕТ SLA COMPLIANCE RATE (ДОЛЯ В ВАЛЮТЕ ВРЕМЕНИ) ---
        with db.get_connection() as conn:
            # S_MTTR_NORM у нас задан выше (8.0 часов). Используем его, чтобы 
            # нормативы MTTR и SLA ссылались на одно и то же бизнес-правило.
            sla_df = pd.read_sql_query(get_sla_metrics_query(sla_hours=S_MTTR_NORM), conn)

        # Безопасно извлекаем данные. Если задач еще нет, ставим 100% авансом.
        if not sla_df.empty and sla_df.iloc[0]['total_resolved'] > 0:
            total_resolved = sla_df.iloc[0]['total_resolved']
            within_sla = sla_df.iloc[0]['within_sla']
            
            # Защита от пустого значения (NaN), если база вернула NULL
            if pd.isna(within_sla): within_sla = 0 
                
            sla_compliance_rate = (within_sla / total_resolved) * 100
        else:
            sla_compliance_rate = 100.0
            
        # Красим метрику в красный, если мы выполняем норматив реже, чем в 90% случаев
        sla_color = "normal" if sla_compliance_rate >= 90 else "inverse"

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

        # --- ОТРИСОВКА ДАШБОРДА (Сетка с обновленным неймингом) ---
        
        # Строка 1: Управление рисками, качеством (MTTR/SLA) и временем
        r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
        
        with r1_c1:
            st.metric("Risk - Предотвращено риска", f"{total_risk_days_saved:,.0f} дн.".replace(',', ' '), 
                      delta="MTTD: <24ч", delta_color="off",
                      help=f"Суммарный лаг обнаружения. 365 дн. для неликвидов и {AVG_MANUAL_DETECTION_DAYS} дн. для активного стока.")
            
        with r1_c2:
            st.metric("MTTR - Время устранения", mttr_display, 
                      delta=f"SLA: {S_MTTR_NORM}ч", delta_color=mttr_delta_color,
                      help=f"Median Time to Resolve: типичное время реакции склада на проблему.")
            
        with r1_c3:
            st.metric("SLA Compliance", f"{sla_compliance_rate:.1f}%", 
                      delta="Цель: >90%", delta_color=sla_color,
                      help=f"Доля инцидентов, устраненных в рамках норматива ({S_MTTR_NORM} ч.). Показывает стабильность процессов.")
            
        with r1_c4:
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

    # --- НОВЫЙ РАЗДЕЛ: ЗДОРОВЬЕ ВИТРИНЫ (Ghosting Rate) ---
    st.subheader("👻 Уровень мерцания сайта (Ghosting Rate)")
    st.caption("Показывает количество товаров, которые вчера имели положительный остаток, а сегодня бесследно пропали с витрины.")
    
    with db.get_connection() as conn:
        ghost_query = """
            WITH DailyStocks AS (
                SELECT 
                    SUBSTR(report_timestamp, 1, 10) as date,
                    item_name,
                    quantity
                FROM stocks
                WHERE quantity > 0
            )
            SELECT 
                d1.date as "Дата",
                COUNT(d1.item_name) as "Пропало на следующий день"
            FROM DailyStocks d1
            LEFT JOIN DailyStocks d2 
                ON d1.item_name = d2.item_name 
                AND date(d2.date) = date(d1.date, '+1 day')
            WHERE d2.item_name IS NULL 
              AND d1.date < date('now', 'localtime')
            GROUP BY d1.date
            ORDER BY d1.date ASC
        """
        df_ghosts = pd.read_sql_query(ghost_query, conn)
        
    if not df_ghosts.empty and len(df_ghosts) > 1:
        st.bar_chart(df_ghosts.set_index("Дата"), color="#ff4b4b")
        avg_ghosts = df_ghosts["Пропало на следующий день"].mean()
        if avg_ghosts > 0:
            st.info(f"💡 В среднем **{int(avg_ghosts)}** товаров исчезает с сайта каждый день. На вкладке '📦 Склад' вы вручную классифицируете: легальная ли это продажа под ноль, или 'Баг 1С'.")
    else:
        st.success("Мало данных для построения графика мерцания или сайт работает идеально.")
    
    st.divider()

    st.write("---")
    st.write("**💸 Оценка упущенной выгоды (Risk Value Modeling)**")

    # 1. Запрашиваем из базы сумму ущерба всех зафиксированных багов 1С
    with db.get_connection() as conn:
        bug_impact_query = "SELECT SUM(financial_impact) FROM anomaly_log WHERE anomaly_type = 'Скрыт с витрины (Баг)'"
        total_max_risk_result = conn.execute(bug_impact_query).fetchone()[0]

    # Если багов еще нет, SQL вернет None. Защищаемся от ошибки, ставя 0.
    total_max_risk = total_max_risk_result if total_max_risk_result else 0

    # 2. Создаем колонки для красоты: слева ползунок, справа цифра
    risk_col1, risk_col2 = st.columns([1, 2])
    
    with risk_col1:
        # Streamlit slider: ползунок от 1% до 100%
        online_share = st.slider(
            "Доля сайта в продажах (%)",
            min_value=1,
            max_value=100,
            value=5, # 5% - наша консервативная стартовая оценка
            help="Коэффициент атрибуции канала. Какая доля из этих товаров реально продалась бы через онлайн-витрину?"
        )

    # 3. Математика: умножаем общий риск на выбранный процент
    adjusted_lost_sales = total_max_risk * (online_share / 100)

    with risk_col2:
        # Красиво выводим метрику
        st.metric(
            "Скрытая упущенная выгода (Ghosting Loss)",
            f"{adjusted_lost_sales:,.0f} ₽".replace(',', ' '),
            help=f"Формула: Максимальный риск ({total_max_risk:,.0f} ₽) × {online_share}% вероятности продажи"
        )

    st.write("---")

    # --- РАЗДЕЛ 1: SYSTEM IQ (Дневная динамика) ---
    st.subheader("🤖 System IQ (Daily Health & Intel)")
    with db.get_connection() as conn:
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
    with db.get_connection() as conn:
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
    
    with db.get_connection() as conn:
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
                    with db.get_connection() as conn:
                        conn.execute("""
                            UPDATE anomaly_log 
                            SET comment = '[BUG] ' || IFNULL(comment, 'Ошибка классификации/UI') 
                            WHERE id = ?
                        """, (row['id'],))
                        conn.commit()
                    st.rerun()
            
            # 8. КНОПКА ВОЗВРАТА В АНОМАЛИИ
            if c[7].button("↩️", key=f"restore_hist_{row['id']}", help="Отменить решение и вернуть в Аномалии"):
                with db.get_connection() as conn:
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
    
    with db.get_connection() as conn:
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
                with db.get_connection() as conn:
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
                with db.get_connection() as conn:
                    conn.execute("DELETE FROM anomaly_log WHERE id = ?", (row['id'],))
                    conn.commit()
                # 2. Возвращаем на экран Аномалий
                if row['item_name'] in st.session_state.dismissed_names:
                    st.session_state.dismissed_names.remove(row['item_name'])
                
                st.toast(f"Товар возвращен во вкладку Аномалии!")
                st.rerun()
                
            st.divider() # Рисует аккуратную линию между записями