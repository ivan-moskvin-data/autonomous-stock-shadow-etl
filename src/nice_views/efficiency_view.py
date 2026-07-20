"""
efficiency_view.py — NiceGUI-версия вкладки «Эффективность».
Полный перенос функционала из src/views/efficiency_view.py.
"""
from nicegui import ui, run as ng_run
import sys
import os
import logging
import pandas as pd

_src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

import db
from nice_views.shared_layout import build_shell
from queries import get_sla_metrics_query

logger = logging.getLogger('shadow_stock.efficiency')

# ─── Константы ────────────────────────────────────────────────────────────────
S_MTTR_NORM       = 8.0   # SLA норматив (часы)
OVERHEAD_MINUTES  = 20
MEDIAN_BATCH_SIZE = 17
TIME_ESCALATION   = 20    # мин. (склад + офис)
COST_PER_SHEET    = 1.2   # ₽
AVG_DETECT_DAYS   = 90    # дней без системы


# ─────────────────────────────────────────────────────────────────────────────
#  Функции получения данных
# ─────────────────────────────────────────────────────────────────────────────

def _kpi_data(include_tests: bool) -> dict | None:
    with db.get_connection() as conn:
        if include_tests:
            q = "SELECT item_name, source, anomaly_type, status, detected_at, resolved_at FROM anomaly_log"
        else:
            q = """
                SELECT item_name, source, anomaly_type, status, detected_at, resolved_at
                FROM anomaly_log
                WHERE anomaly_type NOT IN (
                    'Тестовая запись', 'Системная ошибка',
                    '📦 Плановый приход', '⏳ Догруз с сайта', '🔄 Обновление карточки'
                )
            """
        df = pd.read_sql_query(q, conn)

    if df.empty:
        return None

    total = len(df)

    proactive_mask  = (
        df['source'].isin(['Автоматически', 'Вручную (План)']) &
        (df['anomaly_type'] != 'Успешная сверка')
    )
    proactive_issues = int(proactive_mask.sum())
    proactive_count  = int(df['source'].isin(['Автоматически', 'Вручную (План)']).sum())
    proactive_rate   = proactive_count / total * 100 if total else 0.0

    # MTTR
    resolved = df[
        (df['status'] == 'Закрыта') &
        (df['anomaly_type'] != 'Успешная сверка') &
        df['detected_at'].notnull() &
        df['resolved_at'].notnull()
    ].copy()

    mttr = 0.0
    if not resolved.empty:
        resolved['detected_at'] = pd.to_datetime(resolved['detected_at'])
        resolved['resolved_at'] = pd.to_datetime(resolved['resolved_at'])
        times = (resolved['resolved_at'] - resolved['detected_at']).dt.total_seconds() / 3600.0
        m     = times[times > 0].median()
        mttr  = float(m) if not pd.isna(m) else 0.0

    if 0 < mttr < 1:
        mttr_disp = f"{mttr * 60:.0f} мин."
    else:
        mttr_disp = f"{mttr:.1f} ч."

    # SLA Compliance
    with db.get_connection() as conn:
        sla_df = pd.read_sql_query(get_sla_metrics_query(sla_hours=S_MTTR_NORM), conn)

    sla_rate = 100.0
    if not sla_df.empty and sla_df.iloc[0]['total_resolved'] > 0:
        w = sla_df.iloc[0]['within_sla']
        if pd.isna(w): w = 0
        sla_rate = float(w) / float(sla_df.iloc[0]['total_resolved']) * 100

    # Экономия
    min_per_item = OVERHEAD_MINUTES / MEDIAN_BATCH_SIZE
    routine_h    = (total * min_per_item) / 60
    comms_h      = (proactive_issues * TIME_ESCALATION) / 60
    total_h      = routine_h + comms_h
    h, m_part    = int(total_h), int(round((total_h - int(total_h)) * 60))
    if m_part == 60: h += 1; m_part = 0
    time_str = f"{h} ч. {m_part} мин."

    opex_saved  = total * (COST_PER_SHEET / MEDIAN_BATCH_SIZE)
    sheets      = total / MEDIAN_BATCH_SIZE
    trees       = sheets / 10_000
    risk_days   = proactive_issues * (AVG_DETECT_DAYS - 1)

    return {
        'risk_days':      risk_days,
        'mttr':           mttr,
        'mttr_disp':      mttr_disp,
        'sla_rate':       sla_rate,
        'time_str':       time_str,
        'routine_h':      routine_h,
        'comms_h':        comms_h,
        'proactive_rate': proactive_rate,
        'opex_saved':     opex_saved,
        'sheets':         sheets,
        'trees':          trees,
    }


def _ghosting_data() -> pd.DataFrame:
    """
    Данные по каждому дню парсинга:
    - количество собранных товаров
    - количество уникальных категорий
    - длительность сессии в минутах
    - диагностика: норма / мерцание сайта / сбой парсера
    """
    try:
        with db.get_connection() as conn:
            df = pd.read_sql_query("""
                SELECT
                    DATE(report_timestamp)        AS date,
                    COUNT(*)                      AS items,
                    COUNT(DISTINCT category)      AS cats,
                    ROUND(
                        (julianday(MAX(report_timestamp)) -
                         julianday(MIN(report_timestamp))) * 1440
                    )                             AS duration_min
                FROM stocks
                GROUP BY DATE(report_timestamp)
                ORDER BY date ASC
            """, conn)

        if df.empty:
            return df

        # Медиана по категориям за все дни (эталон нормального парсинга)
        median_cats  = df['cats'].median()
        median_items = df['items'].median()
        # Порог «нормально» = 80% от медианы
        cat_threshold  = median_cats  * 0.80
        item_threshold = median_items * 0.80

        def _diagnose(row):
            if row['items'] >= item_threshold:
                return '✅ Норма'
            # Мало товаров. Смотрим на категории:
            if row['cats'] >= cat_threshold:
                # Категории обошёл нормально, товаров мало → сайт мерцал
                return '🌐 Мерцание сайта'
            # Категорий тоже мало → парсер упал рано
            return '🤖 Сбой парсера'

        df['diagnosis'] = df.apply(_diagnose, axis=1)

        # Переименовываем для отображения
        df = df.rename(columns={
            'date':         'Дата',
            'items':        'Собрано товаров',
            'cats':         'Разделов посещено',
            'duration_min': 'Время сессии (мин)',
        })
        return df
    except Exception:
        return pd.DataFrame()


def _max_risk() -> float:
    try:
        with db.get_connection() as conn:
            r = conn.execute(
                "SELECT SUM(financial_impact) FROM anomaly_log "
                "WHERE anomaly_type = 'Скрыт с витрины (Баг)'"
            ).fetchone()[0]
        return float(r) if r else 0.0
    except Exception:
        return 0.0


def _verified_today() -> dict:
    """
    Считает сколько позиций сверено вручную сегодня.
    Источники: 'Вручную (План)' и 'Вручную (Инцидент)'.
    Возвращает dict с ключами: count, last_time.
    """
    try:
        with db.get_connection() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) AS cnt,
                    MAX(detected_at) AS last_ts
                FROM anomaly_log
                WHERE source LIKE 'Вручную%'
                  AND DATE(detected_at) = DATE('now', 'localtime')
            """).fetchone()
        count = int(row[0]) if row and row[0] else 0
        last  = str(row[1])[:16] if row and row[1] else None
        return {'count': count, 'last_time': last}
    except Exception:
        return {'count': 0, 'last_time': None}


def _iq_data(include_tests: bool) -> pd.DataFrame:
    where = "" if include_tests else "WHERE anomaly_type != 'Тестовая запись'"
    try:
        with db.get_connection() as conn:
            return pd.read_sql_query(f"""
                SELECT
                    DATE(detected_at) AS Day,
                    CASE
                        WHEN IFNULL(comment,'') LIKE '%[BUG]%'                  THEN 'Failures'
                        WHEN source = 'Автоматически (Нейро-приемка)'           THEN '✨ AI Auto-Receive'
                        WHEN anomaly_type IN (
                            '📦 Плановый приход','⏳ Догруз с сайта',
                            '🔄 Обновление карточки')                            THEN 'Routine (Manual)'
                        WHEN anomaly_type = 'Системная ошибка'                  THEN 'Failures'
                        WHEN anomaly_type = 'Тестовая запись'                   THEN 'Debug'
                        ELSE 'Signal (Anomalies)'
                    END AS cat,
                    COUNT(*) AS count
                FROM anomaly_log {where}
                GROUP BY 1, 2 ORDER BY Day ASC
            """, conn)
    except Exception:
        return pd.DataFrame()


def _fa_data(include_tests: bool) -> pd.DataFrame:
    where = (
        "WHERE anomaly_type != 'Успешная сверка' "
        "AND IFNULL(comment,'') NOT LIKE '%[BUG]%' "
        "AND source != 'Автоматически (Нейро-приемка)'"
    )
    if not include_tests:
        where += " AND anomaly_type NOT IN ('Тестовая запись')"
    try:
        with db.get_connection() as conn:
            return pd.read_sql_query(f"""
                SELECT DATE(detected_at) AS Day,
                       anomaly_type, COUNT(*) AS count
                FROM anomaly_log {where}
                GROUP BY 1, 2 ORDER BY Day ASC
            """, conn)
    except Exception:
        return pd.DataFrame()


def _history_data(include_tests: bool) -> pd.DataFrame:
    subq = ("(SELECT sku FROM stocks s "
            "WHERE s.item_name = a.item_name AND sku != '' "
            "ORDER BY report_timestamp DESC LIMIT 1) AS sku")
    if include_tests:
        q = f"""
            SELECT a.id, a.detected_at, a.resolved_at, a.item_name,
                   a.anomaly_type, a.qty_physical, a.source, a.comment, {subq}
            FROM anomaly_log a
            WHERE a.status != 'Открыта'
              AND (
                  a.anomaly_type NOT IN (
                    '📦 Плановый приход','Успешная сверка',
                    '⏳ Догруз с сайта','🔄 Обновление карточки')
                  OR IFNULL(a.comment,'') LIKE '%[BUG]%'
              )
            ORDER BY a.resolved_at DESC LIMIT 50
        """
    else:
        q = f"""
            SELECT a.id, a.detected_at, a.resolved_at, a.item_name,
                   a.anomaly_type, a.qty_physical, a.source, a.comment, {subq}
            FROM anomaly_log a
            WHERE a.status != 'Открыта'
              AND a.anomaly_type NOT IN (
                'Тестовая запись','Системная ошибка',
                '📦 Плановый приход','Успешная сверка',
                '⏳ Догруз с сайта','🔄 Обновление карточки')
              AND IFNULL(a.comment,'') NOT LIKE '%[BUG]%'
            ORDER BY a.resolved_at DESC LIMIT 50
        """
    try:
        with db.get_connection() as conn:
            return pd.read_sql_query(q, conn)
    except Exception:
        return pd.DataFrame()


def _legal_data() -> pd.DataFrame:
    subq = ("(SELECT sku FROM stocks s "
            "WHERE s.item_name = a.item_name AND sku != '' "
            "ORDER BY report_timestamp DESC LIMIT 1) AS sku")
    try:
        with db.get_connection() as conn:
            return pd.read_sql_query(f"""
                SELECT a.id, a.detected_at, a.item_name, a.anomaly_type, a.comment,
                       (a.qty_system - a.qty_physical) AS delta, {subq}
                FROM anomaly_log a
                WHERE a.anomaly_type IN (
                    '📦 Плановый приход','Успешная сверка',
                    '⏳ Догруз с сайта','🔄 Обновление карточки')
                  AND IFNULL(a.comment,'') NOT LIKE '%[BUG]%'
                  AND IFNULL(a.comment,'') NOT LIKE '🔗 Склеено (старое имя)%'
                ORDER BY a.detected_at DESC LIMIT 50
            """, conn)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Вспомогательные рендеры
# ─────────────────────────────────────────────────────────────────────────────

def _kpi_card(value: str, label: str, color: str, hint: str = ''):
    with ui.card().classes('p-4').style(
        f'background:#171717; border-left:3px solid {color}; min-width:180px;'
    ):
        ui.label(value).classes('text-white text-2xl font-bold')
        ui.label(label).style('color:#9ca3af; font-size:0.78rem;')
        if hint:
            ui.label(hint).style('color:#6b7280; font-size:0.7rem;')


def _area_echart(pivot: pd.DataFrame, color_map: dict) -> None:
    dates  = pivot.index.tolist()
    series = []
    for col in pivot.columns:
        c = color_map.get(col, '#94a3b8')
        series.append({
            'name':      col,
            'type':      'line',
            'stack':     'total',
            'areaStyle': {'opacity': 0.35},
            'smooth':    True,
            'data':      [int(v) for v in pivot[col].tolist()],
            'itemStyle': {'color': c},
            'lineStyle': {'color': c},
        })
    ui.echart({
        'backgroundColor': 'transparent',
        'legend':  {'data': [s['name'] for s in series],
                    'textStyle': {'color': '#9ca3af'}, 'type': 'scroll'},
        'tooltip': {'trigger': 'axis'},
        'xAxis':   {'type': 'category', 'data': dates,
                    'axisLabel': {'color': '#6b7280'},
                    'axisLine':  {'lineStyle': {'color': '#2a2a2a'}}},
        'yAxis':   {'type': 'value',
                    'axisLabel':  {'color': '#6b7280'},
                    'splitLine':  {'lineStyle': {'color': '#1f1f1f'}}},
        'series':  series,
        'grid':    {'left': '3%', 'right': '4%', 'bottom': '3%', 'containLabel': True},
    }).classes('w-full').style('height:300px;')


def _history_row(row, refresh_cb):
    is_bug = '[BUG]' in str(row.get('comment', ''))
    anom_t = str(row.get('anomaly_type', ''))
    sku    = str(row['sku']) if pd.notna(row.get('sku')) and row.get('sku') else '—'
    date_s = str(row.get('resolved_at') or row.get('detected_at', ''))[:16]
    cmt    = str(row.get('comment', '')) if pd.notna(row.get('comment')) else ''

    tc = '#d1d5db'
    if is_bug or anom_t in ('Утеря', 'Тихая отмена'):         tc = '#ef4444'
    elif anom_t == 'Успешная сверка':                          tc = '#22c55e'
    elif anom_t in ('Излишек', 'Пересорт (Склад)', 'Пересорт (1С)'): tc = '#f59e0b'

    with ui.row().classes('w-full items-center gap-2 px-3 py-2 flex-wrap').style(
        'border-bottom:1px solid #1a1a1a;'
    ):
        ui.label(date_s).style('color:#9ca3af; font-size:0.73rem; min-width:115px; flex-shrink:0;')
        ui.label(f'🏷 {sku}').classes('font-mono').style(
            'color:#6b7280; font-size:0.75rem; min-width:100px; flex-shrink:0;'
        )
        ui.label(str(row.get('item_name',''))).classes('flex-1 text-sm text-white')
        ui.label(('🔴 ' if is_bug else '') + anom_t).style(
            f'color:{tc}; font-size:0.78rem; min-width:130px; flex-shrink:0;'
        )
        ui.label(str(row.get('qty_physical', ''))).style(
            'color:#9ca3af; font-size:0.78rem; min-width:35px; flex-shrink:0;'
        )
        ui.label((cmt[:45] + '…') if len(cmt) > 45 else cmt).style(
            'color:#6b7280; font-size:0.72rem; min-width:100px; flex-shrink:0;'
        )

        with ui.row().classes('gap-1 flex-shrink-0'):
            if is_bug:
                ui.button('✅', on_click=None).props('flat size=xs').set_enabled(False) \
                  .tooltip('Уже баг')
            else:
                def _mark_bug(_id=int(row['id'])):
                    with db.get_connection() as conn:
                        conn.execute(
                            "UPDATE anomaly_log "
                            "SET comment='[BUG] '||IFNULL(comment,'Ошибка UI') "
                            "WHERE id=?", (_id,)
                        )
                        conn.commit()
                    ui.notify('🚨 Помечено как баг', type='info')
                    refresh_cb()

                ui.button('🚨', on_click=_mark_bug).props('flat size=xs color=negative') \
                  .tooltip('Пометить как баг UI/системы')

            def _restore(_id=int(row['id'])):
                with db.get_connection() as conn:
                    conn.execute('DELETE FROM anomaly_log WHERE id=?', (_id,))
                    conn.commit()
                ui.notify('↩️ Возвращено в Аномалии', type='info')
                refresh_cb()

            ui.button('↩️', on_click=_restore).props('flat size=xs') \
              .tooltip('Вернуть в Аномалии')


def _legal_row(row, refresh_cb):
    anom_t  = str(row.get('anomaly_type', ''))
    sku     = str(row['sku']) if pd.notna(row.get('sku')) and row.get('sku') else '—'
    date_s  = str(row.get('detected_at', ''))[:16]
    cmt     = str(row.get('comment', '')) if pd.notna(row.get('comment')) else ''
    delta_v = int(row['delta']) if pd.notna(row.get('delta')) else 0
    delta_s = f'+{delta_v} шт.' if delta_v > 0 else f'{delta_v} шт.'

    tc = '#d1d5db'
    if anom_t == 'Успешная сверка':       tc = '#22c55e'
    elif anom_t == '⏳ Догруз с сайта':   tc = '#f59e0b'
    elif anom_t == '🔄 Обновление карточки': tc = '#a78bfa'

    with ui.row().classes('w-full items-center gap-2 px-3 py-2 flex-wrap').style(
        'border-bottom:1px solid #1a1a1a;'
    ):
        ui.label(date_s).style('color:#9ca3af; font-size:0.73rem; min-width:115px; flex-shrink:0;')
        ui.label(f'🏷 {sku}').classes('font-mono').style(
            'color:#6b7280; font-size:0.75rem; min-width:100px; flex-shrink:0;'
        )
        ui.label(str(row.get('item_name',''))).classes('flex-1 text-sm text-white')
        if cmt:
            ui.label(f'💬 {cmt[:40]}').style('color:#6b7280; font-size:0.72rem; flex-shrink:0;')

        with ui.row().classes('items-center gap-1 flex-shrink-0'):
            ui.label(anom_t).style(f'color:{tc}; font-size:0.78rem;')
            if delta_v != 0:
                ui.label(delta_s).style('color:#9ca3af; font-size:0.72rem;')

        with ui.row().classes('gap-1 flex-shrink-0'):
            def _bug_leg(_id=int(row['id'])):
                with db.get_connection() as conn:
                    conn.execute(
                        "UPDATE anomaly_log "
                        "SET comment='[BUG] '||IFNULL(comment,'Ошибочная легализация') "
                        "WHERE id=?", (_id,)
                    )
                    conn.commit()
                ui.notify('🚨 Помечено как баг', type='info')
                refresh_cb()

            def _restore_leg(_id=int(row['id'])):
                with db.get_connection() as conn:
                    conn.execute('DELETE FROM anomaly_log WHERE id=?', (_id,))
                    conn.commit()
                ui.notify('↩️ Возвращено в Аномалии', type='info')
                refresh_cb()

            ui.button('🚨', on_click=_bug_leg).props('flat size=xs color=negative') \
              .tooltip('Ошибся кнопкой? → в баги')
            ui.button('↩️', on_click=_restore_leg).props('flat size=xs') \
              .tooltip('Вернуть в Аномалии')


# ─────────────────────────────────────────────────────────────────────────────
#  Страница
# ─────────────────────────────────────────────────────────────────────────────

def setup_page():

    @ui.page('/efficiency')
    async def efficiency_page():
        logger.info('efficiency_page() handler entered')
        build_shell('/efficiency')

        # Per-client state
        include_state = [False]
        iq_sel_state  = [None]
        fa_sel_state  = [None]
        _d_kpi        = [None]
        _d_iq         = [None]
        _d_fa         = [None]
        _d_hist       = [None]
        _d_legal      = [None]
        _d_ghost      = [None]
        _d_max_risk   = [0.0]
        _d_verified   = [None]

        # ── Outer container — returned to browser immediately ─────────────────
        with ui.column().classes('w-full p-4 gap-6').style(
            'background:#0d0d0d; min-height:100vh;'
        ):
            with ui.row().classes('w-full items-center justify-between flex-wrap gap-2'):
                ui.label('🎯 KPI: Эффективность и Качество (Lean Model)').classes(
                    'text-white text-xl font-bold'
                )
                include_cb = ui.checkbox('🧪 Тестовые данные и баги', value=False)

            ui.separator().style('background:#2a2a2a;')

            # Spinner — visible until data loads
            spinner_row = ui.row().classes('w-full items-center justify-center p-12 gap-4')
            with spinner_row:
                ui.spinner(size='xl').props('color=primary')
                ui.label('Загрузка данных...').style('color:#9ca3af; font-size:1rem;')

            # Content placeholder — shown after load
            content_col = ui.column().classes('w-full gap-6')
            content_col.set_visibility(False)

        # ── Deferred loader — fires AFTER page is sent to browser ─────────────
        async def _load_and_render():
            import asyncio as _aio
            logger.info('efficiency: parallel data load started')
            try:
                (
                    _d_kpi[0], _d_iq[0], _d_fa[0],
                    _d_hist[0], _d_legal[0], _d_ghost[0],
                    _d_max_risk[0], _d_verified[0],
                ) = await _aio.gather(
                    ng_run.io_bound(_kpi_data,     include_state[0]),
                    ng_run.io_bound(_iq_data,      include_state[0]),
                    ng_run.io_bound(_fa_data,      include_state[0]),
                    ng_run.io_bound(_history_data, include_state[0]),
                    ng_run.io_bound(_legal_data),
                    ng_run.io_bound(_ghosting_data),
                    ng_run.io_bound(_max_risk),
                    ng_run.io_bound(_verified_today),
                )
            except Exception:
                logger.exception('efficiency: data load failed')
                spinner_row.set_visibility(False)
                with content_col:
                    ui.label('❌ Ошибка загрузки данных. Перезагрузите страницу.').classes('text-red-400 text-lg')
                content_col.set_visibility(True)
                return

            logger.info('efficiency: data loaded OK, building UI')
            spinner_row.set_visibility(False)

            IQ_COLORS = {
                'Routine (Manual)':    '#3b82f6',
                '✨ AI Auto-Receive': '#8b5cf6',
                'Debug':              '#6b7280',
                'Failures':           '#ef4444',
                'Signal (Anomalies)': '#22c55e',
            }

            with content_col:

                # ═══ 1. KPI-карточки ═══════════════════════════════════════════
                @ui.refreshable
                def render_kpi():
                    if _d_kpi[0] is not None:
                        kpi = _d_kpi[0]; _d_kpi[0] = None
                    else:
                        kpi = _kpi_data(include_state[0])
                    if not kpi:
                        with ui.card().classes('w-full p-4').style(
                            'background:#171717; border:1px solid #2a2a2a;'
                        ):
                            ui.label('ℹ️ Пока нет данных для расчёта KPI.').classes('text-gray-400')
                        return
                    mttr_ok = kpi['mttr'] <= S_MTTR_NORM
                    sla_ok  = kpi['sla_rate'] >= 90
                    with ui.row().classes('gap-4 flex-wrap'):
                        _kpi_card(
                            f"{kpi['risk_days']:,.0f} дн.".replace(',', ' '),
                            'Risk — Предотвращено риска', '#f97316',
                            f'MTTD: <24ч  (→ {AVG_DETECT_DAYS} дн. без системы)'
                        )
                        _kpi_card(
                            kpi['mttr_disp'],
                            'MTTR — Время устранения',
                            '#22c55e' if mttr_ok else '#ef4444',
                            f'SLA: {S_MTTR_NORM}ч  ▸  {"✅ OK" if mttr_ok else "❌ Превышен"}'
                        )
                        _kpi_card(
                            f"{kpi['sla_rate']:.1f}%",
                            'SLA Compliance',
                            '#22c55e' if sla_ok else '#ef4444',
                            f'Цель: >90%  ▸  {"✅ OK" if sla_ok else "❌ Ниже нормы"}'
                        )
                        _kpi_card(
                            kpi['time_str'],
                            'Time — Сэкономлено времени',
                            '#60a5fa',
                            f'Рутина: {kpi["routine_h"]:.2f}ч + Комм: {kpi["comms_h"]:.2f}ч'
                        )
                    with ui.row().classes('gap-4 flex-wrap mt-2'):
                        with ui.card().classes('p-4').style(
                            'background:#171717; border-left:3px solid #a78bfa; min-width:180px;'
                        ):
                            ui.label(f"{kpi['proactive_rate']:.1f}%").classes('text-white text-2xl font-bold')
                            ui.label('PR — Проактивность').style('color:#9ca3af; font-size:0.78rem;')
                            ui.linear_progress(kpi['proactive_rate'] / 100).props('color=deep-purple')
                        _kpi_card(
                            f"{kpi['opex_saved']:.2f} ₽",
                            'OPEX — Снижение затрат', '#34d399',
                            f'Бумага: {kpi["sheets"]:.1f} листов А4'
                        )
                        _kpi_card(
                            f"{kpi['trees']:.5f} 🌳",
                            'ESG — Eco Impact', '#22c55e',
                            f'{kpi["sheets"]:.1f} стр. × {COST_PER_SHEET}₽/лист'
                        )
                        # ── Сверено сегодня ──────────────────────────────
                        _vt = _d_verified[0] if _d_verified[0] is not None else _verified_today()
                        _vt_hint = (
                            f'Последняя: {_vt["last_time"]}'
                            if _vt['last_time'] else 'Ещё не было сегодня'
                        )
                        _vt_color = '#34d399' if _vt['count'] > 0 else '#6b7280'
                        _kpi_card(
                            f"{_vt['count']} поз.",
                            '✅ Сверено сегодня', _vt_color,
                            _vt_hint
                        )

                render_kpi()
                ui.separator().style('background:#2a2a2a;')

                # ═══ 2. Ghosting Rate ════════════════════════════════════════
                ui.label('👻 Диагностика парсинга (Ghosting Rate)').classes(
                    'text-white text-xl font-bold'
                )
                ui.label(
                    'Каждый день парсинга: сколько товаров и разделов собрал парсер '
                    'и какова вероятная причина отклонений.'
                ).style('color:#9ca3af; font-size:0.82rem;')

                df_ghost = _d_ghost[0] if _d_ghost[0] is not None else pd.DataFrame()

                if df_ghost.empty or len(df_ghost) < 2:
                    with ui.card().classes('w-full p-3').style(
                        'background:#052e16; border:1px solid #22c55e;'
                    ):
                        ui.label(
                            '✅ Мало данных для анализа.'
                        ).classes('text-green-400 text-sm')
                else:
                    # ── Бар-чарт «Собрано товаров по дням» ──────────────────
                    # Цвет зависит от диагноза
                    color_map = {
                        '✅ Норма':          '#22c55e',
                        '🌐 Мерцание сайта': '#f97316',
                        '🤖 Сбой парсера':  '#ef4444',
                    }
                    bar_colors = [
                        color_map.get(d, '#6b7280')
                        for d in df_ghost['diagnosis'].tolist()
                    ]
                    ui.echart({
                        'backgroundColor': 'transparent',
                        'tooltip': {
                            'trigger': 'axis',
                            'formatter': '{b}<br/>Товаров: {c}'
                        },
                        'xAxis': {
                            'type': 'category',
                            'data': df_ghost['Дата'].tolist(),
                            'axisLabel': {'color': '#6b7280', 'rotate': 30},
                            'axisLine':  {'lineStyle': {'color': '#2a2a2a'}}
                        },
                        'yAxis': {
                            'type': 'value',
                            'axisLabel':  {'color': '#6b7280'},
                            'splitLine':  {'lineStyle': {'color': '#1f1f1f'}}
                        },
                        'series': [{
                            'type': 'bar',
                            'name': 'Товаров собрано',
                            'data': [
                                {'value': v, 'itemStyle': {'color': c}}
                                for v, c in zip(
                                    df_ghost['Собрано товаров'].tolist(),
                                    bar_colors
                                )
                            ],
                            'itemStyle': {'borderRadius': 3},
                        }],
                        'grid': {'left':'3%','right':'4%','bottom':'12%','containLabel':True},
                    }).classes('w-full').style('height:280px;')

                    # ── Легенда ─────────────────────────────────────────────
                    with ui.row().classes('gap-4 flex-wrap mb-2'):
                        for label, color in color_map.items():
                            with ui.row().classes('items-center gap-1'):
                                ui.element('div').style(
                                    f'width:12px; height:12px; border-radius:2px; background:{color};'
                                )
                                ui.label(label).style('color:#9ca3af; font-size:0.8rem;')

                    # ── Детальная таблица ────────────────────────────────────
                    ui.label('📋 Детализация по дням').classes(
                        'text-white font-semibold mt-2'
                    )

                    # Цветовой рендер диагноза через cellStyle
                    diag_col = {
                        'field': 'diagnosis',
                        'headerName': 'Диагноз',
                        'sortable': True,
                        'width': 200,
                        'cellClassRules': {
                            'text-green-400':  "value.startsWith('✅')",
                            'text-orange-400': "value.startsWith('🌐')",
                            'text-red-400':    "value.startsWith('🤖')",
                        },
                    }

                    table_df = df_ghost[['Дата', 'Собрано товаров', 'Разделов посещено',
                                         'Время сессии (мин)', 'diagnosis']].copy()
                    table_df = table_df.rename(columns={'diagnosis': 'Диагноз'})

                    ui.aggrid({
                        'columnDefs': [
                            {'field': 'Дата',                 'headerName': 'Дата',                 'sortable': True, 'width': 120},
                            {'field': 'Собрано товаров',      'headerName': 'Собрано товаров',      'sortable': True, 'width': 150},
                            {'field': 'Разделов посещено',    'headerName': 'Разделов посещено',    'sortable': True, 'width': 170},
                            {'field': 'Время сессии (мин)',   'headerName': 'Время сессии (мин)',   'sortable': True, 'width': 170},
                            diag_col,
                        ],
                        'rowData':   table_df.to_dict('records'),
                        'rowClassRules': {
                            'bg-orange-950': "data.Диагноз.startsWith('🌐')",
                            'bg-red-950':    "data.Диагноз.startsWith('🤖')",
                        },
                    }).classes('w-full ag-theme-balham-dark').style('height:400px;')

                    # ── Итоговый инсайт ──────────────────────────────────────
                    n_flicker = (df_ghost['diagnosis'] == '🌐 Мерцание сайта').sum()
                    n_parser  = (df_ghost['diagnosis'] == '🤖 Сбой парсера').sum()
                    if n_flicker > 0 or n_parser > 0:
                        with ui.card().classes('w-full p-3 mt-2').style(
                            'background:#1e1b4b; border:1px solid #818cf8;'
                        ):
                            parts = []
                            if n_flicker:
                                parts.append(f'🌐 {n_flicker} дн. — вероятное мерцание сайта')
                            if n_parser:
                                parts.append(f'🤖 {n_parser} дн. — вероятный сбой парсера/ноута')
                            ui.label(
                                '💡 Обнаружены аномальные дни: ' + ' | '.join(parts)
                            ).classes('text-indigo-200 text-sm')

                ui.separator().style('background:#2a2a2a;')

                # ═══ 3. Risk Value Modeling ══════════════════════════════════
                ui.label('💸 Оценка упущенной выгоды (Risk Value Modeling)').classes(
                    'text-white text-xl font-bold'
                )
                total_max_risk = _d_max_risk[0] or 0.0
                with ui.row().classes('gap-6 items-start flex-wrap'):
                    with ui.column().classes('gap-2').style('min-width:260px;'):
                        ui.label('Доля сайта в продажах (%)').style(
                            'color:#d1d5db; font-size:0.9rem;'
                        )
                        pct_lbl = ui.label('5%').style('color:#9ca3af; font-size:0.85rem;')
                        slider  = ui.slider(min=1, max=100, value=5).classes('w-full')
                    init_adj = total_max_risk * 0.05
                    with ui.card().classes('p-6').style(
                        'background:#171717; border-left:3px solid #f97316;'
                    ):
                        risk_val_lbl  = ui.label(
                            f'{init_adj:,.0f} ₽'.replace(',', ' ')
                        ).classes('text-white text-2xl font-bold')
                        ui.label('Скрытая упущенная выгода (Ghosting Loss)').style(
                            'color:#9ca3af; font-size:0.8rem;'
                        )
                        risk_hint_lbl = ui.label(
                            f'Мак. риск: {total_max_risk:,.0f} ₽ × 5%'.replace(',', ' ')
                        ).style('color:#6b7280; font-size:0.72rem;')

                def on_slide(e):
                    v   = int(e.value)
                    adj = total_max_risk * (v / 100)
                    risk_val_lbl.set_text(f'{adj:,.0f} ₽'.replace(',', ' '))
                    pct_lbl.set_text(f'{v}%')
                    risk_hint_lbl.set_text(
                        f'Мак. риск: {total_max_risk:,.0f} ₽ × {v}%'.replace(',', ' ')
                    )
                slider.on_value_change(on_slide)
                ui.separator().style('background:#2a2a2a;')

                # ═══ 4. System IQ ════════════════════════════════════════════
                ui.label('🤖 System IQ (Daily Health & Intel)').classes(
                    'text-white text-xl font-bold'
                )
                @ui.refreshable
                def render_iq():
                    if _d_iq[0] is not None:
                        df_iq = _d_iq[0]; _d_iq[0] = None
                    else:
                        df_iq = _iq_data(include_state[0])
                    if df_iq.empty:
                        ui.label('Нет данных.').style('color:#6b7280;')
                        return
                    all_cats  = sorted(df_iq['cat'].unique().tolist())
                    curr_sel  = [c for c in (iq_sel_state[0] or all_cats) if c in all_cats]
                    def on_iq_sel(e):
                        iq_sel_state[0] = e.value
                        render_iq.refresh()
                    sel = ui.select(
                        options=all_cats, value=curr_sel, multiple=True,
                        label='Детализация System IQ',
                    ).classes('w-full')
                    sel.on_value_change(on_iq_sel)
                    filtered = df_iq[df_iq['cat'].isin(sel.value)] if sel.value else pd.DataFrame()
                    if not filtered.empty:
                        pivot = filtered.pivot(index='Day', columns='cat', values='count').fillna(0)
                        _area_echart(pivot, IQ_COLORS)
                render_iq()
                ui.separator().style('background:#2a2a2a;')

                # ═══ 5. Feature Adoption ════════════════════════════════════
                ui.label('🖱️ Feature Adoption (Ручная нагрузка на менеджера)').classes(
                    'text-white text-xl font-bold'
                )
                @ui.refreshable
                def render_fa():
                    if _d_fa[0] is not None:
                        df_fa = _d_fa[0]; _d_fa[0] = None
                    else:
                        df_fa = _fa_data(include_state[0])
                    if df_fa.empty:
                        ui.label('Нет данных.').style('color:#6b7280;')
                        return
                    all_types = sorted(df_fa['anomaly_type'].unique().tolist())
                    curr_sel  = [t for t in (fa_sel_state[0] or all_types) if t in all_types]
                    def on_fa_sel(e):
                        fa_sel_state[0] = e.value
                        render_fa.refresh()
                    sel = ui.select(
                        options=all_types, value=curr_sel, multiple=True,
                        label='Динамика кликов',
                    ).classes('w-full')
                    sel.on_value_change(on_fa_sel)
                    filtered = df_fa[df_fa['anomaly_type'].isin(sel.value)] if sel.value else pd.DataFrame()
                    if not filtered.empty:
                        pivot = filtered.pivot(index='Day', columns='anomaly_type', values='count').fillna(0)
                        _area_echart(pivot, {})
                render_fa()
                ui.separator().style('background:#2a2a2a;')

                # ═══ 6. История инцидентов ═════════════════════════════════════════
                ui.label('📜 История выявленных проблем (последние 50)').classes(
                    'text-white text-xl font-bold'
                )
                @ui.refreshable
                def render_hist():
                    if _d_hist[0] is not None:
                        df_h = _d_hist[0]; _d_hist[0] = None
                    else:
                        df_h = _history_data(include_state[0])
                    if df_h.empty:
                        ui.label('В истории нет инцидентов.').style('color:#6b7280;')
                        return
                    disp = df_h[['detected_at','resolved_at','item_name',
                                 'anomaly_type','qty_physical','source','comment']].copy()
                    disp.columns = ['Обнаружено','Закрыто','Товар','Тип','Кол-во','Источник','Комментарий']
                    disp = disp.fillna('').astype(str)
                    ui.aggrid({
                        'columnDefs': [
                            {'field': 'Обнаружено',  'width': 150, 'sortable': True},
                            {'field': 'Закрыто',     'width': 150, 'sortable': True},
                            {'field': 'Товар',       'flex': 3,    'sortable': True, 'filter': True},
                            {'field': 'Тип',         'flex': 2,    'sortable': True, 'filter': True},
                            {'field': 'Кол-во',      'width': 90,  'sortable': True},
                            {'field': 'Источник',   'width': 140, 'sortable': True},
                            {'field': 'Комментарий',  'flex': 2},
                        ],
                        'rowData':   disp.to_dict('records'),
                        'domLayout': 'autoHeight',
                        'pagination': True,
                        'paginationPageSize': 20,
                    }).classes('w-full ag-theme-balham-dark')
                render_hist()
                ui.separator().style('background:#2a2a2a;')

                # ═══ 7. Жуၲнал рутины ════════════════════════════════════════════════
                ui.label('🙈 Журнал рутины (Легализованные аномалии)').classes(
                    'text-white text-xl font-bold'
                )
                @ui.refreshable
                def render_legal():
                    if _d_legal[0] is not None:
                        df_l = _d_legal[0]; _d_legal[0] = None
                    else:
                        df_l = _legal_data()
                    if df_l.empty:
                        ui.label('Жуၲнал рутины пуст.').style('color:#6b7280;')
                        return
                    disp = df_l[['detected_at','item_name','anomaly_type','delta','comment']].copy()
                    disp.columns = ['Дата','Товар','Тип','Δ','Комментарий']
                    disp = disp.fillna('').astype(str)
                    ui.aggrid({
                        'columnDefs': [
                            {'field': 'Дата',        'width': 150, 'sortable': True},
                            {'field': 'Товар',       'flex': 3,    'sortable': True, 'filter': True},
                            {'field': 'Тип',         'flex': 2,    'sortable': True, 'filter': True},
                            {'field': 'Δ',           'width': 70},
                            {'field': 'Комментарий', 'flex': 2},
                        ],
                        'rowData':   disp.to_dict('records'),
                        'domLayout': 'autoHeight',
                        'pagination': True,
                        'paginationPageSize': 20,
                    }).classes('w-full ag-theme-balham-dark')
                render_legal()

                # Обработчик чекбокса
                async def on_tests_toggle():
                    include_state[0] = include_cb.value
                    iq_sel_state[0]  = None
                    fa_sel_state[0]  = None
                    render_kpi.refresh()
                    render_iq.refresh()
                    render_fa.refresh()
                    render_hist.refresh()
                include_cb.on_value_change(on_tests_toggle)

            content_col.set_visibility(True)

        ui.timer(0.1, _load_and_render, once=True)
