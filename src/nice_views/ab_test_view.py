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
            df = pd.read_sql_query("""
                SELECT
                    f.*,
                    (SELECT quantity FROM stocks s
                     WHERE s.item_name = f.item_name
                     ORDER BY report_timestamp DESC LIMIT 1) AS current_qty
                FROM ai_forecasts f
                ORDER BY f.created_at DESC
            """, conn)

            if df.empty:
                return df

            # Батч-загрузка истории остатков для всех товаров одним запросом
            # (один SELECT вместо N отдельных — минимальная нагрузка)
            item_list = df['item_name'].dropna().unique().tolist()
            placeholders = ','.join('?' * len(item_list))
            hist_df = pd.read_sql_query(f"""
                SELECT
                    item_name,
                    SUBSTR(report_timestamp, 1, 10) AS date,
                    quantity
                FROM stocks
                WHERE item_name IN ({placeholders})
                  AND report_timestamp >= date('now', '-30 days', 'localtime')
                GROUP BY item_name, SUBSTR(report_timestamp, 1, 10)
                HAVING report_timestamp = MAX(report_timestamp)
                ORDER BY item_name, date ASC
            """, conn, params=item_list)

            # Группируем историю по товару → список [qty, qty, ...]
            history_map: dict = {}
            for name, grp in hist_df.groupby('item_name'):
                history_map[name] = grp['quantity'].tolist()

            df['sparkline'] = df['item_name'].map(
                lambda n: history_map.get(n, [])
            )
            return df

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
#  Оценка точности прогнозов
# ─────────────────────────────────────────────────────────────────────────────

def _check_forecast_accuracy() -> None:
    """
    Проверяет точность прогнозов, у которых прошло достаточно времени.

    Алгоритм:
    1. Берём прогнозы в статусе '⏳ Наблюдение' или '🔴 Товар отсутствует',
       созданные более lead_time_days дней назад (прошло достаточно времени).
    2. Смотрим что реально произошло с остатком товара после даты прогноза:
       - Если остаток достиг 0 в пределах ±TOLERANCE_DAYS от predicted_zero_date
         → '✅ Точный прогноз'
       - Если прогноз предсказывал обнуление, но товар всё ещё есть и дата
         уже прошла → '📉 Упущенная выгода' (прогноз был верен, но не купили вовремя)
       - Иначе оставляем текущий статус (наблюдаем дальше)
    """
    TOLERANCE_DAYS = 3  # ±3 дня — «точный прогноз»

    try:
        config = db.CONFIG
        lead_time = config['ai']['lead_time_days']

        with db.get_connection() as conn:
            forecasts = pd.read_sql_query(f"""
                SELECT id, item_name, sku, predicted_zero_date, avg_daily_sales,
                       created_at, status
                FROM ai_forecasts
                WHERE status IN ('⏳ Наблюдение', '🔴 Товар отсутствует')
                  AND date(created_at, '+{lead_time} days') <= date('now', 'localtime')
                  AND predicted_zero_date IS NOT NULL
            """, conn)

            if forecasts.empty:
                return

            for _, row in forecasts.iterrows():
                item_name   = row['item_name']
                pred_date   = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
                created_at  = pd.to_datetime(row['created_at'], errors='coerce')
                db_id       = row['id']
                avg_sales   = float(row['avg_daily_sales'] or 0)

                if pd.isna(pred_date) or pd.isna(created_at):
                    continue

                # Берём историю остатков после даты прогноза
                window_start = created_at.strftime('%Y-%m-%d')
                window_end   = (pred_date + pd.Timedelta(days=TOLERANCE_DAYS + 5)).strftime('%Y-%m-%d')

                hist = pd.read_sql_query("""
                    SELECT SUBSTR(report_timestamp, 1, 10) AS date, quantity
                    FROM stocks
                    WHERE item_name = ?
                      AND SUBSTR(report_timestamp, 1, 10) BETWEEN ? AND ?
                    GROUP BY SUBSTR(report_timestamp, 1, 10)
                    HAVING report_timestamp = MAX(report_timestamp)
                    ORDER BY date ASC
                """, conn, params=(item_name, window_start, window_end))

                if hist.empty:
                    continue

                # Ищем первый день когда остаток упал до 0 или очень низко (< avg/2)
                threshold = max(1, avg_sales * 0.5) if avg_sales > 0 else 1
                zero_rows = hist[hist['quantity'] <= threshold]

                if not zero_rows.empty:
                    actual_zero_date = pd.to_datetime(zero_rows.iloc[0]['date'])
                    diff_days = abs((actual_zero_date - pred_date).days)

                    if diff_days <= TOLERANCE_DAYS:
                        # Прогноз точный!
                        conn.execute(
                            "UPDATE ai_forecasts SET status='✅ Точный прогноз' WHERE id=?",
                            (db_id,)
                        )
                    # Если diff > TOLERANCE — прогноз ошибся, оставляем текущий статус

            conn.commit()

    except Exception:
        logger.exception('_check_forecast_accuracy error')


def _load_accuracy_stats() -> dict:
    """
    Возвращает агрегированную статистику точности прогнозов.

    Возвращает dict с ключами:
      - total_evaluated: кол-во оценённых прогнозов
      - accurate_count:  кол-во точных (✅)
      - accuracy_pct:    % точных
      - mape:            MAPE по дням (средняя абс. ошибка / среднее предсказание × 100)
      - weekly_trend:    list of dicts {week, accurate, total} для графика
    """
    empty = {
        'total_evaluated': 0, 'accurate_count': 0,
        'accuracy_pct': 0.0, 'mape': None, 'weekly_trend': [],
    }
    try:
        with db.get_connection() as conn:
            # Все прогнозы в терминальных статусах (кроме 🔄 — пересчитан)
            terminal = pd.read_sql_query("""
                SELECT id, item_name, predicted_zero_date, created_at, status,
                       avg_daily_sales, lead_time_days
                FROM ai_forecasts
                WHERE status IN (
                    '✅ Точный прогноз', '📉 Упущенная выгода', '🔴 Товар отсутствует'
                )
                ORDER BY created_at DESC
            """, conn)

            if terminal.empty:
                return empty

            total = len(terminal)
            accurate = (terminal['status'] == '✅ Точный прогноз').sum()
            accuracy_pct = round(accurate / total * 100, 1) if total > 0 else 0.0

            # MAPE: только для точных прогнозов — сравниваем predicted_zero_date
            # с реальной датой обнуления (если она известна — берём из stocks)
            mape_errors = []
            for _, row in terminal[terminal['status'] == '✅ Точный прогноз'].iterrows():
                pred_date  = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
                created_at = pd.to_datetime(row['created_at'], errors='coerce')
                avg_sales  = float(row['avg_daily_sales'] or 0)
                if pd.isna(pred_date) or pd.isna(created_at) or avg_sales == 0:
                    continue

                # Ищем фактическое обнуление в stocks
                hist = pd.read_sql_query("""
                    SELECT SUBSTR(report_timestamp, 1, 10) AS date, quantity
                    FROM stocks
                    WHERE item_name = ?
                      AND SUBSTR(report_timestamp, 1, 10) >= ?
                    GROUP BY SUBSTR(report_timestamp, 1, 10)
                    HAVING report_timestamp = MAX(report_timestamp)
                    ORDER BY date ASC
                    LIMIT 30
                """, conn, params=(row['item_name'], created_at.strftime('%Y-%m-%d')))

                threshold = max(1, avg_sales * 0.5)
                zero_rows = hist[hist['quantity'] <= threshold]
                if zero_rows.empty:
                    continue

                actual_zero = pd.to_datetime(zero_rows.iloc[0]['date'])
                predicted_days = (pred_date - created_at).days
                actual_days    = (actual_zero - created_at).days
                if predicted_days > 0:
                    ape = abs(actual_days - predicted_days) / predicted_days * 100
                    mape_errors.append(ape)

            mape = round(sum(mape_errors) / len(mape_errors), 1) if mape_errors else None

            # Недельный тренд
            terminal['week'] = pd.to_datetime(
                terminal['created_at'], errors='coerce'
            ).dt.to_period('W').astype(str)

            weekly = (
                terminal.groupby('week')
                .apply(lambda g: pd.Series({
                    'total':    len(g),
                    'accurate': (g['status'] == '✅ Точный прогноз').sum(),
                }))
                .reset_index()
                .sort_values('week')
                .tail(12)  # последние 12 недель
            )
            weekly_trend = weekly.to_dict('records')

            return {
                'total_evaluated': int(total),
                'accurate_count':  int(accurate),
                'accuracy_pct':    accuracy_pct,
                'mape':            mape,
                'weekly_trend':    weekly_trend,
            }

    except Exception:
        logger.exception('_load_accuracy_stats error')
        return empty


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

                # ── Обновляем статусы и проверяем точность прогнозов ──────
                await ng_run.io_bound(_verify_shadow_forecasts)
                await ng_run.io_bound(_check_forecast_accuracy)
                acc_stats = await ng_run.io_bound(_load_accuracy_stats)
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
                    # ── Метрики (упущенная выгода + заморозка) ────────────
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

                    # ── Accuracy Dashboard ────────────────────────────────
                    ui.label('🎯 Точность прогнозов (Accuracy Dashboard)').classes(
                        'text-white text-lg font-semibold'
                    )

                    acc_total    = acc_stats['total_evaluated']
                    acc_accurate = acc_stats['accurate_count']
                    acc_pct      = acc_stats['accuracy_pct']
                    acc_mape     = acc_stats['mape']
                    acc_trend    = acc_stats['weekly_trend']

                    if acc_total < 3:
                        with ui.card().classes('w-full p-3').style(
                            'background:#111827; border:1px dashed #374151;'
                        ):
                            ui.label(
                                '⏳ Данных пока недостаточно для оценки точности. '
                                f'Оценено прогнозов: {acc_total}. '
                                f'Нужно минимум 3 завершённых прогноза — система накапливает историю.'
                            ).style('color:#6b7280; font-size:0.85rem;')
                    else:
                        # Карточки точности
                        with ui.row().classes('gap-4 flex-wrap w-full'):
                            # Точность %
                            acc_color = (
                                '#22c55e' if acc_pct >= 70
                                else '#f59e0b' if acc_pct >= 40
                                else '#ef4444'
                            )
                            with ui.card().classes('p-5').style(
                                f'background:#171717; border-left:3px solid {acc_color};'
                            ):
                                ui.label(f'{acc_pct:.1f}%').classes(
                                    'text-white text-2xl font-bold'
                                )
                                ui.label('🎯 Точность (Forecast Accuracy)').style(
                                    'color:#9ca3af; font-size:0.8rem;'
                                )
                                ui.label(
                                    '% прогнозов, попавших в ±3 дня от факта'
                                ).style('color:#6b7280; font-size:0.72rem;')

                            # MAPE
                            mape_txt = f'{acc_mape:.1f}%' if acc_mape is not None else '—'
                            mape_color = (
                                '#22c55e' if acc_mape is not None and acc_mape < 15
                                else '#f59e0b' if acc_mape is not None and acc_mape < 35
                                else '#ef4444'
                            )
                            with ui.card().classes('p-5').style(
                                f'background:#171717; border-left:3px solid {mape_color};'
                            ):
                                ui.label(mape_txt).classes(
                                    'text-white text-2xl font-bold'
                                )
                                ui.label('📐 Ошибка прогноза (MAPE)').style(
                                    'color:#9ca3af; font-size:0.8rem;'
                                )
                                ui.label(
                                    'Средн. % отклонения от фактической даты'
                                ).style('color:#6b7280; font-size:0.72rem;')

                            # Оценено / точных
                            with ui.card().classes('p-5').style(
                                'background:#171717; border-left:3px solid #818cf8;'
                            ):
                                ui.label(f'{acc_accurate} / {acc_total}').classes(
                                    'text-white text-2xl font-bold'
                                )
                                ui.label('📊 Точных / Оценено прогнозов').style(
                                    'color:#9ca3af; font-size:0.8rem;'
                                )
                                ui.label(
                                    'Накопленная история верификации'
                                ).style('color:#6b7280; font-size:0.72rem;')

                        # Недельный тренд (стековая гистограмма)
                        if acc_trend:
                            weeks      = [r['week'] for r in acc_trend]
                            acc_vals   = [int(r['accurate']) for r in acc_trend]
                            inac_vals  = [int(r['total']) - int(r['accurate']) for r in acc_trend]

                            ui.echart({
                                'backgroundColor': 'transparent',
                                'tooltip': {'trigger': 'axis', 'axisPointer': {'type': 'shadow'}},
                                'legend': {
                                    'data': ['✅ Точные', '❌ Неточные'],
                                    'textStyle': {'color': '#9ca3af'},
                                },
                                'grid': {'left': '3%', 'right': '4%', 'bottom': '3%', 'containLabel': True},
                                'xAxis': {
                                    'type': 'category', 'data': weeks,
                                    'axisLabel': {'color': '#6b7280', 'rotate': 30, 'fontSize': 10},
                                    'axisLine': {'lineStyle': {'color': '#374151'}},
                                },
                                'yAxis': {
                                    'type': 'value', 'minInterval': 1,
                                    'axisLabel': {'color': '#6b7280'},
                                    'splitLine': {'lineStyle': {'color': '#1f2937'}},
                                },
                                'series': [
                                    {
                                        'name': '✅ Точные',
                                        'type': 'bar', 'stack': 'total',
                                        'data': acc_vals,
                                        'itemStyle': {'color': '#22c55e'},
                                        'label': {'show': True, 'position': 'inside', 'color': '#fff', 'fontSize': 10},
                                    },
                                    {
                                        'name': '❌ Неточные',
                                        'type': 'bar', 'stack': 'total',
                                        'data': inac_vals,
                                        'itemStyle': {'color': '#374151'},
                                        'label': {'show': True, 'position': 'inside', 'color': '#9ca3af', 'fontSize': 10},
                                    },
                                ],
                            }).classes('w-full').style('height:220px;')

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
                        'sparkline',
                        'abc_category',
                    ]].copy() if 'abc_category' in df_fc.columns else df_fc[[
                        'created_at', 'item_name', 'current_qty',
                        'predicted_zero_date', 'recommended_qty',
                        'avg_daily_sales', 'lead_time_days', 'safety_stock',
                        'reason', 'status',
                        'lost_sales_value', 'overstock_value',
                        'sparkline',
                    ]].copy().assign(abc_category='?')

                    disp['current_qty'] = disp['current_qty'].fillna(0).astype(int)
                    disp['created_at']  = disp['created_at'].astype(str).str[:10]
                    disp['abc_category'] = disp['abc_category'].fillna('C')
                    disp['lost_sales_value']  = disp['lost_sales_value'].fillna(0)
                    disp['overstock_value']   = disp['overstock_value'].fillna(0)
                    disp['Упущ. выручка (₽)'] = disp['lost_sales_value'].apply(_fmt_rub)
                    disp['Заморожено (₽)']    = disp['overstock_value'].apply(_fmt_rub)
                    # sparkline: преобразуем в список чисел (на случай если пришли NaN)
                    disp['sparkline'] = disp['sparkline'].apply(
                        lambda v: [int(x) for x in v] if isinstance(v, list) else []
                    )
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
                        'abc_category':         'ABC',
                    })

                    col_defs = [
                        {'field': 'Дата',  'headerName': 'Дата', 'flex': 1, 'sortable': True},
                        {
                            'field': 'ABC', 'headerName': 'ABC', 'flex': 1,
                            'sortable': True,
                            'cellStyle': {
                                'function': (
                                    "const v=params.value||'';"
                                    "if(v==='A')return{color:'#fbbf24',fontWeight:'800',fontSize:'1rem',textAlign:'center'};"
                                    "if(v==='B')return{color:'#38bdf8',fontWeight:'700',textAlign:'center'};"
                                    "return{color:'#6b7280',textAlign:'center'};"
                                )
                            },
                            'headerTooltip': 'A — критически важные (80% оборота), B — умеренно важные (15%), C — низкоприоритетные (5%)',
                        },
                        {'field': 'Товар', 'headerName': 'Товар', 'flex': 3, 'sortable': True, 'filter': True, 'resizable': True},
                        {'field': 'Остаток', 'headerName': 'Остаток', 'flex': 1, 'type': 'numericColumn'},
                        {
                            'field': 'sparkline',
                            'headerName': 'Тренд (30д)',
                            'flex': 2,
                            'cellRenderer': 'agSparklineCellRenderer',
                            'cellRendererParams': {
                                'sparklineOptions': {
                                    'type': 'line',
                                    'line': {'stroke': '#22c55e', 'strokeWidth': 1.5},
                                    'marker': {'enabled': False},
                                    'crosshairs': {
                                        'xLine': {'enabled': True, 'lineDash': 'dash', 'stroke': '#374151'},
                                        'yLine': {'enabled': True, 'lineDash': 'dash', 'stroke': '#374151'},
                                    },
                                    'tooltip': {
                                        'enabled': True,
                                        'renderer': 'function(params){return {title:"",content:params.yValue+" шт"}}',
                                    },
                                    'fill': 'rgba(34,197,94,0.08)',
                                    'padding': {'top': 6, 'bottom': 6},
                                },
                            },
                        },
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
                        'rowHeight':          60,
                        'pagination':         True,
                        'paginationPageSize': 15,
                    }).classes('w-full ag-theme-balham-dark').style('height:500px;')

                ui.separator().style('background:#2a2a2a;')

                # ── Статус автоматизации ──────────────────────────────────
                ui.label('⚙️ Управление AI-анализом').classes(
                    'text-white text-lg font-semibold'
                )

                has_pending = _AI_PENDING_FLAG.exists()
                today_count = _forecasts_today()

                # Читаем дату последнего парсинга
                try:
                    _last_run_cfg = db.CONFIG.get('paths', {})
                    _base_dir = Path(__file__).resolve().parent.parent.parent
                    _last_run_path = _base_dir / _last_run_cfg.get('last_run_file', 'logs/last_run.txt')
                    last_parse_date = _last_run_path.read_text(encoding='utf-8').strip() \
                        if _last_run_path.exists() else None
                except Exception:
                    last_parse_date = None

                with ui.row().classes('gap-4 flex-wrap w-full'):
                    # ── Блок «Автоматический режим» ────────────────────────
                    with ui.card().classes('p-4 flex-1').style(
                        'background:#111827; border:1px solid #1f2937; min-width:280px;'
                    ):
                        ui.label('🤖 Автоматический режим').classes(
                            'text-white font-semibold mb-2'
                        )
                        ui.label(
                            'После каждого успешного парсинга autostart.py '
                            'автоматически запускает src/ai_services.py — '
                            'прогнозы обновляются без участия пользователя.'
                        ).style('color:#6b7280; font-size:0.8rem;')

                        ui.separator().style('background:#1f2937; margin:8px 0;')

                        if last_parse_date:
                            ui.label(f'📅 Последний парсинг: {last_parse_date}').style(
                                'color:#9ca3af; font-size:0.82rem;'
                            )

                        if has_pending:
                            # Парсер отработал, но ai_forecaster не успел
                            with ui.row().classes('items-center gap-2 mt-1'):
                                ui.icon('warning', size='sm').style('color:#f59e0b;')
                                ui.label(
                                    'Парсер собрал данные, но AI-анализ ещё не выполнен. '
                                    'Возможно, ai_forecaster.py упал — запустите вручную.'
                                ).style('color:#fbbf24; font-size:0.82rem;')
                        elif today_count > 0:
                            with ui.row().classes('items-center gap-2 mt-1'):
                                ui.icon('check_circle', size='sm').style('color:#22c55e;')
                                ui.label(
                                    f'Автоанализ выполнен сегодня: {today_count} прогнозов в базе.'
                                ).style('color:#86efac; font-size:0.82rem;')
                        else:
                            with ui.row().classes('items-center gap-2 mt-1'):
                                ui.icon('schedule', size='sm').style('color:#6b7280;')
                                ui.label(
                                    'Анализ сегодня ещё не запускался. '
                                    'Ждём следующего парсинга.'
                                ).style('color:#6b7280; font-size:0.82rem;')

                    # ── Блок «Ручной запуск» ───────────────────────────────
                    with ui.card().classes('p-4 flex-1').style(
                        'background:#111827; border:1px solid #1f2937; min-width:280px;'
                    ):
                        ui.label('🖱️ Ручной запуск').classes(
                            'text-white font-semibold mb-2'
                        )
                        ui.label(
                            'Используйте если: AI-скрипт упал автоматически, '
                            'хотите пересчитать прогнозы с новыми параметрами, '
                            'или просто проверить систему.'
                        ).style('color:#6b7280; font-size:0.8rem;')

                        ui.separator().style('background:#1f2937; margin:8px 0;')

                        status_lbl = ui.label('').style(
                            'color:#818cf8; font-weight:600; font-size:0.85rem;'
                        )
                        status_lbl.set_visibility(False)

                        async def do_forecast():
                            forecast_btn.set_enabled(False)
                            status_lbl.set_text('🤖 ИИ анализирует графики продаж…')
                            status_lbl.set_visibility(True)
                            try:
                                result = await ng_run.io_bound(ai_services.run_batch_forecast)

                                if result == 'no_key':
                                    ui.notify(
                                        '❌ API-ключ не найден! Проверьте secrets.toml.',
                                        type='negative', timeout=0
                                    )
                                elif result == 'empty':
                                    ui.notify(
                                        '⚠️ Нет товаров для анализа — '
                                        'нет снижений остатков за последние 30 дней.',
                                        type='warning'
                                    )
                                    if _AI_PENDING_FLAG.exists():
                                        _AI_PENDING_FLAG.unlink()
                                elif isinstance(result, str) and result.startswith('error_'):
                                    err = result.split('_', 1)[1]
                                    ui.notify(
                                        f'❌ Ошибка AI: {err}',
                                        type='negative', timeout=0
                                    )
                                elif isinstance(result, str) and result.startswith('ok_'):
                                    count = result.split('_', 1)[1]
                                    ui.notify(
                                        f'✅ Готово! Сгенерировано прогнозов: {count}.',
                                        type='positive'
                                    )
                                    if _AI_PENDING_FLAG.exists():
                                        _AI_PENDING_FLAG.unlink()
                                    await render_main.refresh()
                                else:
                                    ui.notify(f'Результат: {result}', type='info')

                            except Exception as ex:
                                logger.exception('run_batch_forecast error')
                                ui.notify(
                                    f'❌ Критическая ошибка: {ex}',
                                    type='negative', timeout=0
                                )
                            finally:
                                forecast_btn.set_enabled(True)
                                status_lbl.set_visibility(False)

                        btn_label = (
                            '🚀 Запустить анализ (pending данные)'
                            if has_pending else
                            '🔄 Принудительный пересчёт'
                            if today_count > 0 else
                            '🚀 Запустить первичный анализ'
                        )
                        btn_color = 'primary' if has_pending or today_count == 0 else 'secondary'

                        forecast_btn = ui.button(btn_label, on_click=do_forecast) \
                            .props(f'color={btn_color} no-caps') \
                            .classes('w-full mt-1')
                        status_lbl  # rendered after button


            await render_main()

