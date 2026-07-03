"""
ab_test_view.py — NiceGUI-версия вкладки «A/B Тест: AI vs Человек».
Полный перенос функционала из src/views/ab_test_view.py.

verify_shadow_forecasts() инлайнована напрямую (circular import app.py невозможен).
ai_services.run_batch_forecast() вызывается через run.io_bound().
"""
from nicegui import ui, run as ng_run
import sys
import os
import logging
import pandas as pd
from pathlib import Path

_src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

import db
import ai_services
from nice_views.shared_layout import build_shell

logger = logging.getLogger('shadow_stock.abtest')

_AI_PENDING_FLAG = Path(_src_dir).parent / 'logs' / 'ai_pending.flag'


# ─────────────────────────────────────────────────────────────────────────────
#  Инлайн-копия verify_shadow_forecasts (без app.py — circular import)
# ─────────────────────────────────────────────────────────────────────────────

def _verify_shadow_forecasts() -> None:
    """
    Обновляет статусы активных прогнозов по текущим остаткам.
    Логика из app.verify_shadow_forecasts(), без зависимости от Streamlit/app.py.
    """
    try:
        config = db.CONFIG
        with db.get_connection() as conn:
            forecasts = pd.read_sql_query("""
                SELECT * FROM ai_forecasts
                WHERE status NOT IN (
                    '📉 Упущенная выгода', '✅ Точный прогноз', '🔄 Пересчитан ИИ'
                )
            """, conn)

            if forecasts.empty:
                return

            latest_inv = db.load_inventory()
            if latest_inv.empty:
                return

            today = pd.Timestamp.now().normalize()

            for _, row in forecasts.iterrows():
                item_name = row['item_name']
                sku       = row['sku']
                db_id     = row['id']

                match = pd.DataFrame()
                if pd.notna(sku) and str(sku).strip():
                    match = latest_inv[latest_inv['Артикул'] == sku]
                if match.empty:
                    match = latest_inv[latest_inv['Наименование'] == item_name]
                if match.empty:
                    continue

                curr_qty  = float(match.iloc[0]['Остаток'])
                price     = float(match.iloc[0]['Цена'])
                avg_sales = float(row['avg_daily_sales'])

                # Пересчёт если изменился lead_time в конфиге
                current_lead = config['ai']['lead_time_days']
                forecast_lead = int(row['lead_time_days']) if row['lead_time_days'] else 14
                if forecast_lead != current_lead:
                    base_demand = int(curr_qty + avg_sales * current_lead)
                    safety      = int(avg_sales * 0.2)
                    rec_qty     = base_demand + safety
                    days_to_z   = round(curr_qty / avg_sales, 1) if avg_sales > 0 else 999.0
                    zero_date   = (today + pd.Timedelta(days=int(days_to_z))).strftime('%Y-%m-%d')
                    conn.execute("""
                        UPDATE ai_forecasts
                        SET predicted_zero_date=?, recommended_qty=?,
                            lead_time_days=?, safety_stock=?, base_demand=?,
                            needs_recalc=0
                        WHERE id=?
                    """, (zero_date, rec_qty, current_lead, safety, base_demand - safety, db_id))
                    continue

                pred_date = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
                if pd.isna(pred_date):
                    pred_date = today + pd.Timedelta(days=30)

                if curr_qty <= 0:
                    effective = min(today, pred_date)
                    days_lost = max(1, (today - effective).days)
                    lost_val  = days_lost * avg_sales * price
                    conn.execute("""
                        UPDATE ai_forecasts
                        SET status='🔴 Товар отсутствует',
                            lost_sales_value=?, overstock_value=0
                        WHERE id=?
                    """, (lost_val, db_id))
                    continue

                if curr_qty > (avg_sales * 60):
                    overstock_qty = curr_qty - (avg_sales * 44)
                    overstock_val = max(0, overstock_qty * price)
                    conn.execute("""
                        UPDATE ai_forecasts
                        SET status='🧊 Перезатарка',
                            overstock_value=?, lost_sales_value=0
                        WHERE id=?
                    """, (overstock_val, db_id))
                else:
                    conn.execute(
                        "UPDATE ai_forecasts SET status='⏳ Наблюдение' WHERE id=?",
                        (db_id,)
                    )

            conn.commit()

    except Exception:
        logger.exception('_verify_shadow_forecasts error')


# ─────────────────────────────────────────────────────────────────────────────
#  Загрузка данных
# ─────────────────────────────────────────────────────────────────────────────

def _days_in_db() -> int:
    try:
        with db.get_connection() as conn:
            return conn.execute(
                "SELECT COUNT(DISTINCT SUBSTR(report_timestamp,1,10)) FROM stocks"
            ).fetchone()[0] or 0
    except Exception:
        return 0


def _forecasts_today() -> int:
    try:
        with db.get_connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM ai_forecasts "
                "WHERE date(created_at) = date('now','localtime')"
            ).fetchone()[0] or 0
    except Exception:
        return 0


def _load_forecasts() -> pd.DataFrame:
    try:
        with db.get_connection() as conn:
            return pd.read_sql_query("""
                SELECT
                    f.*,
                    (SELECT quantity FROM stocks s
                     WHERE s.item_name = f.item_name
                     ORDER BY report_timestamp DESC LIMIT 1) AS current_qty
                FROM ai_forecasts f
                ORDER BY f.created_at DESC
            """, conn)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Вспомогательные UI
# ─────────────────────────────────────────────────────────────────────────────

def _status_color(status: str) -> str:
    if '📉' in status or '🔴' in status:
        return '#ef4444'
    if '🧊' in status:
        return '#38bdf8'
    if '✅' in status:
        return '#22c55e'
    if '⏳' in status or '🔄' in status:
        return '#f59e0b'
    return '#9ca3af'


def _fmt_rub(val) -> str:
    try:
        v = float(val)
        return f"{v:,.0f} ₽".replace(',', '\u202f') if v > 0 else ''
    except Exception:
        return ''


# ─────────────────────────────────────────────────────────────────────────────
#  Страница
# ─────────────────────────────────────────────────────────────────────────────

def setup_page():

    @ui.page('/abtest')
    async def abtest_page():
        logger.info('abtest_page() handler entered')
        build_shell('/abtest')

        with ui.column().classes('w-full p-4 gap-6').style(
            'background:#0d0d0d; min-height:100vh;'
        ):
            # ── Заголовок ─────────────────────────────────────────────────
            ui.label('⚖️ A/B Тест: AI-прогноз vs Человеческие решения').classes(
                'text-white text-2xl font-bold'
            )
            ui.label(
                'Теневой режим: алгоритм делает прогнозы закупок и сверяет их '
                'с реальными действиями менеджеров. Позволяет оценить упущенную '
                'выгоду без вмешательства в бизнес-процессы.'
            ).style('color:#9ca3af; font-size:0.85rem;')

            ui.separator().style('background:#2a2a2a;')

            # ══════════════════════════════════════════════════════════════
            # Основной refreshable
            # ══════════════════════════════════════════════════════════════
            @ui.refreshable
            async def render_main():

                # ── Cold Start индикатор ───────────────────────────────────
                days = await ng_run.io_bound(_days_in_db)
                if days < 30:
                    with ui.card().classes('w-full p-4').style(
                        'background:#1c1917; border:1px solid #a16207;'
                    ):
                        ui.label(
                            f'⚠️ Модель в стадии «прогрева» (Cold Start): '
                            f'накоплено {days} из 30 необходимых дней. '
                            'ИИ экстраполирует короткие тренды — возможна повышенная погрешность.'
                        ).classes('text-yellow-300 text-sm')
                else:
                    with ui.card().classes('w-full p-4').style(
                        'background:#052e16; border:1px solid #22c55e;'
                    ):
                        ui.label(
                            f'✅ Модель обучена: накоплено данных за {days} дней. '
                            'Точность прогнозов оптимальна.'
                        ).classes('text-green-400 text-sm')

                # ── Обновляем статусы прогнозов ───────────────────────────
                await ng_run.io_bound(_verify_shadow_forecasts)
                df_fc = await ng_run.io_bound(_load_forecasts)

                # ── Нет прогнозов ─────────────────────────────────────────
                if df_fc.empty:
                    with ui.card().classes('w-full p-4').style(
                        'background:#111111; border:1px solid #2a2a2a;'
                    ):
                        ui.label(
                            'ℹ️ Пока нет активных прогнозов. '
                            'Нажмите кнопку ниже, чтобы запустить AI-анализ.'
                        ).classes('text-gray-400')
                else:
                    # ── Метрики ───────────────────────────────────────────
                    total_lost      = float(df_fc['lost_sales_value'].fillna(0).sum())
                    total_overstock = float(df_fc['overstock_value'].fillna(0).sum())

                    with ui.row().classes('gap-4 flex-wrap'):
                        with ui.card().classes('p-5').style(
                            'background:#171717; border-left:3px solid #ef4444;'
                        ):
                            ui.label(
                                f"{total_lost:,.0f} ₽".replace(',', '\u202f')
                            ).classes('text-white text-2xl font-bold')
                            ui.label('📉 Упущенная выгода (Prevented Lost Sales)').style(
                                'color:#9ca3af; font-size:0.8rem;'
                            )
                            ui.label(
                                'Сумма потерь из-за несвоевременных закупок'
                            ).style('color:#6b7280; font-size:0.72rem;')

                        with ui.card().classes('p-5').style(
                            'background:#171717; border-left:3px solid #38bdf8;'
                        ):
                            ui.label(
                                f"{total_overstock:,.0f} ₽".replace(',', '\u202f')
                            ).classes('text-white text-2xl font-bold')
                            ui.label('🧊 Замороженный капитал (Cost of Overstock)').style(
                                'color:#9ca3af; font-size:0.8rem;'
                            )
                            ui.label(
                                'Излишки, купленные сверх рекомендаций ИИ'
                            ).style('color:#6b7280; font-size:0.72rem;')

                    ui.separator().style('background:#2a2a2a;')

                    # ── Детализация — журнал прогнозов ────────────────────
                    ui.label('Детализация (Журнал прогнозов и финансовых последствий):').classes(
                        'text-white text-lg font-semibold'
                    )

                    disp = df_fc[[
                        'created_at', 'item_name', 'current_qty',
                        'predicted_zero_date', 'recommended_qty',
                        'avg_daily_sales', 'lead_time_days', 'safety_stock',
                        'reason', 'status',
                        'lost_sales_value', 'overstock_value',
                    ]].copy()

                    disp['current_qty'] = disp['current_qty'].fillna(0).astype(int)
                    disp['created_at']  = disp['created_at'].astype(str).str[:10]
                    disp['lost_sales_value']  = disp['lost_sales_value'].fillna(0)
                    disp['overstock_value']   = disp['overstock_value'].fillna(0)
                    disp['Упущ. выручка (₽)'] = disp['lost_sales_value'].apply(_fmt_rub)
                    disp['Заморожено (₽)']    = disp['overstock_value'].apply(_fmt_rub)
                    disp = disp.drop(columns=['lost_sales_value', 'overstock_value'])
                    disp = disp.rename(columns={
                        'created_at':           'Дата',
                        'item_name':            'Товар',
                        'current_qty':          'Остаток',
                        'predicted_zero_date':  'Обнулится',
                        'recommended_qty':      'Заказ (шт)',
                        'avg_daily_sales':      'Расход/день',
                        'lead_time_days':       'Срок пост.',
                        'safety_stock':         'Страх. запас',
                        'reason':               'Обоснование (AI)',
                        'status':               'Статус',
                    })

                    col_defs = [
                        {'field': 'Дата',       'headerName': 'Дата',        'flex': 1,  'sortable': True},
                        {'field': 'Товар',      'headerName': 'Товар',       'flex': 3,  'sortable': True, 'filter': True, 'resizable': True},
                        {'field': 'Остаток',    'headerName': 'Остаток',     'flex': 1,  'type': 'numericColumn'},
                        {'field': 'Обнулится',  'headerName': 'Обнулится',   'flex': 1,  'sortable': True},
                        {'field': 'Заказ (шт)', 'headerName': 'Заказ (шт)', 'flex': 1,  'type': 'numericColumn'},
                        {'field': 'Расход/день','headerName': 'Расход/д',    'flex': 1,  'type': 'numericColumn'},
                        {'field': 'Срок пост.', 'headerName': 'Срок пост.', 'flex': 1},
                        {'field': 'Страх. запас','headerName': 'Страх. зап.','flex': 1, 'type': 'numericColumn'},
                        {
                            'field': 'Статус', 'headerName': 'Статус', 'flex': 2,
                            'cellStyle': {
                                'function': (
                                    "const s=params.value||'';"
                                    "if(s.includes('📉')||s.includes('🔴'))return{color:'#ef4444',fontWeight:'600'};"
                                    "if(s.includes('🧊'))return{color:'#38bdf8',fontWeight:'600'};"
                                    "if(s.includes('✅'))return{color:'#22c55e',fontWeight:'600'};"
                                    "if(s.includes('⏳')||s.includes('🔄'))return{color:'#f59e0b'};"
                                    "return{color:'#9ca3af'};"
                                )
                            },
                        },
                        {'field': 'Упущ. выручка (₽)', 'headerName': 'Упущ. выручка',
                         'flex': 1, 'cellStyle': {'color': '#ef4444', 'fontWeight': '600'}},
                        {'field': 'Заморожено (₽)',    'headerName': 'Заморожено',
                         'flex': 1, 'cellStyle': {'color': '#38bdf8'}},
                        {'field': 'Обоснование (AI)',  'headerName': 'Обоснование AI',
                         'flex': 3, 'resizable': True,
                         'cellStyle': {'color': '#6b7280', 'fontSize': '0.8rem'}},
                    ]

                    ui.aggrid({
                        'columnDefs':         col_defs,
                        'rowData':            disp.to_dict('records'),
                        'defaultColDef':      {'resizable': True},
                        'pagination':         True,
                        'paginationPageSize': 15,
                    }).classes('w-full ag-theme-balham-dark').style('height:500px;')

                ui.separator().style('background:#2a2a2a;')

                # ── Кнопка запуска AI-анализа ─────────────────────────────
                has_pending   = _AI_PENDING_FLAG.exists()
                today_count   = _forecasts_today()

                if has_pending:
                    with ui.card().classes('w-full p-3').style(
                        'background:#1c1917; border:1px solid #a16207;'
                    ):
                        ui.label(
                            '⚠️ Есть необработанные данные: парсер собрал свежую информацию, '
                            'но AI-анализ ещё не запущен. Нажмите кнопку ниже.'
                        ).classes('text-yellow-300 text-sm')
                    btn_text  = '🚀 Запустить анализ свежих данных'
                    btn_color = 'primary'
                elif today_count > 0:
                    with ui.card().classes('w-full p-3').style(
                        'background:#052e16; border:1px solid #22c55e;'
                    ):
                        ui.label(
                            f'✅ План на сегодня выполнен. '
                            f'В базе {today_count} прогнозов за текущие сутки.'
                        ).classes('text-green-400 text-sm')
                    btn_text  = '🔄 Принудительный пересчёт'
                    btn_color = 'secondary'
                else:
                    btn_text  = '🚀 Запустить первичный анализ'
                    btn_color = 'primary'

                status_lbl = ui.label('').style('color:#818cf8; font-weight:600;')
                status_lbl.set_visibility(False)

                async def do_forecast():
                    forecast_btn.set_enabled(False)
                    status_lbl.set_text('🤖 ИИ анализирует графики продаж…')
                    status_lbl.set_visibility(True)
                    try:
                        result = await ng_run.io_bound(ai_services.run_batch_forecast)

                        if result == 'no_key':
                            ui.notify('❌ Не найден API ключ Gemini!', type='negative', timeout=0)
                        elif result == 'empty':
                            ui.notify('⚠️ Нет товаров для анализа.', type='warning')
                            if has_pending and _AI_PENDING_FLAG.exists():
                                _AI_PENDING_FLAG.unlink()
                        elif isinstance(result, str) and result.startswith('error_'):
                            err = result.split('_', 1)[1]
                            ui.notify(f'❌ Ошибка связи с ИИ: {err}', type='negative', timeout=0)
                        elif isinstance(result, str) and result.startswith('ok_'):
                            count = result.split('_')[1]
                            ui.notify(f'✅ Готово! Сгенерировано прогнозов: {count}.', type='positive')
                            if _AI_PENDING_FLAG.exists():
                                _AI_PENDING_FLAG.unlink()
                            await render_main.refresh()
                        else:
                            ui.notify(f'Результат: {result}', type='info')

                    except Exception as ex:
                        logger.exception('run_batch_forecast error')
                        ui.notify(f'❌ Критическая ошибка: {ex}', type='negative', timeout=0)
                    finally:
                        forecast_btn.set_enabled(True)
                        status_lbl.set_visibility(False)

                forecast_btn = ui.button(btn_text, on_click=do_forecast) \
                    .props(f'color={btn_color} no-caps') \
                    .classes('w-full')

            await render_main()

