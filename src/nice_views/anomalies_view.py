"""
anomalies_view.py — NiceGUI-версия вкладки аномалий.
"""
import logging
logger = logging.getLogger('shadow_stock.anomalies')

from nicegui import ui, run as ng_run
import pandas as pd
import re
import difflib
import traceback
import db
from nice_views.shared_layout import build_shell


# ─────────────────────────────────────────────────────────────────────────────
#  Утилиты
# ─────────────────────────────────────────────────────────────────────────────

def find_best_invoice_match(anomaly_name: str, expected_df: pd.DataFrame):
    if expected_df.empty:
        return None, 0.0
    best_row, max_ratio = None, 0.0
    for _, exp_row in expected_df.iterrows():
        ratio = difflib.SequenceMatcher(
            None,
            str(anomaly_name).lower(),
            str(exp_row['item_name']).lower()
        ).ratio()
        if ratio > max_ratio:
            max_ratio = ratio
            best_row = exp_row
    return best_row, max_ratio


def _get_status_tag(row):
    qty_old    = row.get('Было', 0)
    hist_count = row.get('history_count', 0)
    old_alias  = row.get('old_name_alias', None)
    old_sku    = row.get('old_sku_alias', None)

    if qty_old > 0:
        return '📦 ДОВОЗ',            'Обычное пополнение активного товара.',          'gray'
    elif pd.notna(old_alias) and old_alias:
        return '📝 СМЕНИЛОСЬ ИМЯ',    f'Раньше назывался: {old_alias}.',               'orange'
    elif pd.notna(old_sku) and old_sku:
        return '📝 СМЕНИЛСЯ АРТИКУЛ', f'Старый артикул: {old_sku}.',                   'orange'
    elif hist_count > 0:
        return '🔄 ВОЗВРАТ',          'Товар уже был в базе. Жми «Плановый приход».',  'blue'
    else:
        return '✨ НОВИНКА',           'Абсолютно новый товар.',                        'green'


_TAG_COLORS = {
    'gray':   'text-gray-500',
    'orange': 'text-orange-500',
    'blue':   'text-blue-500',
    'green':  'text-green-600',
}


# ─────────────────────────────────────────────────────────────────────────────
#  Инициализация страницы
# ─────────────────────────────────────────────────────────────────────────────

def setup_page():
    logger.info('anomalies_view.setup_page() called')

    @ui.page('/anomalies')
    async def anomalies_page():
        logger.info('anomalies_page() handler entered')

        dismissed: list[str] = []
        filter_state   = ['all']   # 'all' | 'up' | 'down'
        sort_abs       = [False]   # True = сортировать по |Δ| убыванию
        selected_names: set = set()  # имена выбранных аномалий

        # ── Шапка + сайдбар (общая тёмная разметка) ──────────────────────────
        build_shell('/anomalies')

        # ── refreshable внутри страницы — per-client ─────────────────────────
        @ui.refreshable
        def render_anomalies():
            logger.info('render_anomalies() called')
            try:
                expected_df, df_anomalies, df_inv = _load_anomaly_data()
                _render_content(
                    expected_df, df_anomalies, df_inv,
                    dismissed, render_anomalies,
                    filter_state, sort_abs,
                    selected_names,
                )
                logger.info('render_anomalies() completed OK')
            except Exception as e:
                logger.exception('EXCEPTION inside render_anomalies')
                # Показываем traceback в браузере вместо разрыва соединения
                import traceback as tb_mod
                with ui.card().classes('w-full p-4 bg-red-50 border border-red-300'):
                    ui.label('💥 Ошибка при загрузке страницы').classes('text-red-700 font-bold text-lg mb-2')
                    ui.label(str(e)).classes('text-red-600 mb-2')
                    ui.label(tb_mod.format_exc()).classes('text-xs font-mono whitespace-pre bg-red-100 p-2 w-full')
                    ui.button('🔄 Попробовать снова', on_click=render_anomalies.refresh).props('color=primary')

        logger.info('anomalies_page: calling render_anomalies()')
        with ui.column().classes('w-full max-w-5xl mx-auto p-4'):
            render_anomalies()
        logger.info('anomalies_page: setup complete')


# ─────────────────────────────────────────────────────────────────────────────
#  Основной контент (вызывается из render_anomalies через try/except)
# ─────────────────────────────────────────────────────────────────────────────

def _load_anomaly_data() -> tuple:
    """A1: загружает все данные. Вызывается через run.io_bound, не создаёт UI-элементов."""
    with db.get_connection() as conn:
        try:
            expected_df = pd.read_sql_query(
                "SELECT * FROM expected_deliveries WHERE status = 'Ожидает'", conn
            )
        except Exception:
            expected_df = pd.DataFrame()
    df_anomalies = db.load_anomalies()
    df_inv       = db.load_inventory()
    return expected_df, df_anomalies, df_inv


def _render_content(
    expected_df: pd.DataFrame,
    df_anomalies: pd.DataFrame,
    df_inv: pd.DataFrame,
    dismissed: list,
    refresh_fn,
    filter_state: list | None = None,
    sort_abs: list | None = None,
    selected_names: set | None = None,
):
    if filter_state   is None: filter_state   = ['all']
    if sort_abs       is None: sort_abs       = [False]
    if selected_names is None: selected_names = set()
    active_anom = (
        df_anomalies[~df_anomalies['Наименование'].isin(dismissed)]
        if not df_anomalies.empty else pd.DataFrame()
    )

    # 2. Строгий авто-матчинг 100 %
    if not active_anom.empty and not expected_df.empty:
        arrivals     = active_anom[active_anom['Дельта'] > 0]
        auto_matched = False
        with db.get_connection() as conn:
            for _, anom_row in arrivals.iterrows():
                match = expected_df[
                    (
                        (expected_df['item_name'] == anom_row['Наименование']) |
                        (expected_df['sku']       == anom_row['Артикул'])
                    ) &
                    (expected_df['qty_expected'] == anom_row['Дельта'])
                ]
                if not match.empty:
                    match_id = int(match.iloc[0]['id'])
                    db.save_anomaly_to_db({
                        'item_name':        anom_row['Наименование'],
                        'anomaly_type':     '📦 Плановый приход',
                        'qty_system':       anom_row['Стало'],
                        'qty_physical':     anom_row['Было'],
                        'financial_impact': 0,
                        'source':           'Автоматически (Нейро-приемка)',
                        'status':           'Закрыта',
                        'comment':          f'Авто-матчинг с накладной #{match_id}',
                    })
                    conn.execute(
                        "UPDATE expected_deliveries SET status = 'Принято' WHERE id = ?",
                        (match_id,)
                    )
                    conn.commit()
                    if anom_row['Наименование'] not in dismissed:
                        dismissed.append(anom_row['Наименование'])
                    ui.notify(f"🤖 Авто-приемка: {anom_row['Наименование']}", type='positive')
                    auto_matched = True

        if auto_matched:
            refresh_fn.refresh()
            return

    # 3. Пересчитываем после авто-матчинга
    active_anom = (
        df_anomalies[~df_anomalies['Наименование'].isin(dismissed)]
        if not df_anomalies.empty else pd.DataFrame()
    )

    # 4a. Фильтры и сортировка ─────────────────────────────────────────
    cnt_all = len(active_anom)

    # Вычисляем тег каждой строки один раз (для счётчиков и фильтрации)
    if cnt_all > 0:
        _tags = active_anom.apply(lambda r: _get_status_tag(r)[0], axis=1)
        cnt_up      = int((active_anom['Дельта'] > 0).sum())
        cnt_new     = int((_tags == '✨ НОВИНКА').sum())
        cnt_ret     = int((_tags == '🔄 ВОЗВРАТ').sum())
        cnt_restock = int((_tags == '📦 ДОВОЗ').sum())
        cnt_change  = int(_tags.str.startswith('📝').sum())

        with ui.row().classes('w-full items-center gap-2 flex-wrap mb-2').style(
            'background:#111; border:1px solid #2a2a2a; border-radius:8px; padding:8px 12px;'
        ):
            ui.label('Фильтр:').style('color:#6b7280; font-size:0.8rem; flex-shrink:0;')

            _btn_defs = [
                (f'Все ({cnt_all})',          'all'),
                (f'↑ Рост ({cnt_up})',         'up'),
                (f'✨ Новинки ({cnt_new})',     'new'),
                (f'🔄 Возвраты ({cnt_ret})',    'return'),
                (f'📦 Довоз ({cnt_restock})',   'restock'),
                (f'📝 Изменения ({cnt_change})','change'),
            ]
            for _lbl, _val in _btn_defs:
                _active = filter_state[0] == _val
                def _set_f(_v=_val, _fs=filter_state, _rf=refresh_fn):
                    _fs[0] = _v
                    _rf.refresh()
                ui.button(_lbl, on_click=_set_f).props(
                    f'{"unelevated" if _active else "outline"} '
                    f'color={"primary" if _active else "grey"} '
                    f'no-caps size=sm dense'
                )

            ui.element('div').style('flex:1;')  # spacer

            _sorted = sort_abs[0]
            def _toggle_sort(_sa=sort_abs, _rf=refresh_fn):
                _sa[0] = not _sa[0]
                _rf.refresh()
            ui.button(
                '🔽 По Δ' if _sorted else 'По Δ',
                on_click=_toggle_sort,
            ).props(
                f'{"unelevated color=primary" if _sorted else "outline color=grey"} '
                f'no-caps size=sm dense'
            ).tooltip('Сначала самые крупные поступления')

    # Применяем фильтр
    if not active_anom.empty:
        if filter_state[0] == 'up':
            active_anom = active_anom[active_anom['Дельта'] > 0].copy()
        elif filter_state[0] == 'new':
            _tags2 = active_anom.apply(lambda r: _get_status_tag(r)[0], axis=1)
            active_anom = active_anom[_tags2 == '✨ НОВИНКА'].copy()
        elif filter_state[0] == 'return':
            _tags2 = active_anom.apply(lambda r: _get_status_tag(r)[0], axis=1)
            active_anom = active_anom[_tags2 == '🔄 ВОЗВРАТ'].copy()
        elif filter_state[0] == 'restock':
            _tags2 = active_anom.apply(lambda r: _get_status_tag(r)[0], axis=1)
            active_anom = active_anom[_tags2 == '📦 ДОВОЗ'].copy()
        elif filter_state[0] == 'change':
            _tags2 = active_anom.apply(lambda r: _get_status_tag(r)[0], axis=1)
            active_anom = active_anom[_tags2.str.startswith('📝')].copy()

    # Применяем сортировку по Δ убыванию
    if sort_abs[0] and not active_anom.empty:
        active_anom = active_anom.sort_values('Дельта', ascending=False).copy()

    # 4. Нет аномалий — успех
    if active_anom.empty:
        with ui.card().classes('w-full p-6').style('background:#052e16; border:1px solid #22c55e;'):
            with ui.row().classes('items-center gap-3'):
                ui.icon('check_circle', size='48px').style('color:#22c55e;')
                ui.label('Аномалий нет. 🎉').classes('text-green-400 text-xl font-semibold')
        return

    # 5. Пагинация — рисуем не более PAGE_SIZE карточек за раз
    PAGE_SIZE = 50
    total = len(active_anom)
    page_anom = active_anom.head(PAGE_SIZE)

    if total > PAGE_SIZE:
        with ui.card().classes('w-full p-3').style('background:#1c1400; border:1px solid #f59e0b;'):
            ui.label(
                f'⚠️ Показаны первые {PAGE_SIZE} из {total} аномалий. '
                f'Обработайте их — остальные появятся автоматически.'
            ).classes('text-amber-300 text-sm')
    else:
        ui.label(f'Найдено аномалий: {total}').style('color:#9ca3af; font-size:0.85rem;')

    # 5.5 Кнопка «Плановый приход всех»
    def _mark_all_planned():
        count = 0
        for _, anom_row in page_anom.iterrows():
            if anom_row['Наименование'] in dismissed:
                continue
            db.save_anomaly_to_db({
                'item_name':        anom_row['Наименование'],
                'anomaly_type':     '📦 Плановый приход',
                'qty_system':       anom_row['Стало'],
                'qty_physical':     anom_row['Было'],
                'financial_impact': 0,
                'source':           'Вручную (Массовое подтверждение)',
                'status':           'Закрыта',
                'comment':          'Штатное поступление товара',
            })
            if anom_row['Наименование'] not in dismissed:
                dismissed.append(anom_row['Наименование'])
            count += 1
        ui.notify(f'📦 {count} аномалий закрыты как Плановый приход', type='positive')
        refresh_fn.refresh()

    with ui.row().classes('w-full items-center justify-between mb-3 flex-wrap gap-2'):
        ui.label(f'Показано: {len(page_anom)} из {total}').style('color:#9ca3af; font-size:0.85rem;')
        ui.button(
            f'📦 Плановый приход всех ({len(page_anom)} шт.)',
            on_click=_mark_all_planned,
        ).props('color=positive outline no-caps').tooltip(
            'Пометить все видимые аномалии как Плановый приход'
        )

    # 6. Предзагрузка данных для карточек (1 запрос вместо N)
    # price_dict: Наименование -> Цена
    price_dict: dict = {}
    if not df_inv.empty and 'Цена' in df_inv.columns:
        price_dict = df_inv.dropna(subset=['Наименование']).set_index('Наименование')['Цена'].to_dict()

    # hist_dict: Наименование -> (detected_at, anomaly_type, status)
    # последняя ЗАКРЫТАЯ запись из anomaly_log по каждому товару
    hist_dict: dict = {}
    item_names_cur = page_anom['Наименование'].tolist()
    if item_names_cur:
        _ph = ','.join(['?'] * len(item_names_cur))
        try:
            with db.get_connection() as _hconn:
                _hrows = _hconn.execute(
                    f"""
                    SELECT item_name, detected_at, anomaly_type
                    FROM anomaly_log
                    WHERE item_name IN ({_ph})
                      AND status = 'Закрыта'
                    ORDER BY detected_at DESC
                    """,
                    item_names_cur,
                ).fetchall()
            for _hr in _hrows:
                if _hr[0] not in hist_dict:
                    hist_dict[_hr[0]] = (_hr[1], _hr[2])  # first = most recent
        except Exception:
            pass

    # 6а. Панель массового выбора ─────────────────────────────────────────
    NO_IMPACT_BULK = {'Системная ошибка', '📦 Плановый приход', '⏳ Догруз с сайта', '🔄 Обновление карточки'}
    _bulk_options = [
        '📦 Плановый приход',
        '⏳ Догруз с сайта',
        'Системная ошибка',
        'Утеря',
        'Тихая отмена',
        'Излишек',
        'Пересорт (Склад)',
        'Пересорт (1С)',
    ]
    shown_names = page_anom['Наименование'].tolist()

    sel_count_lbl = ui.label('').style('color:#9ca3af; font-size:0.8rem;')
    bulk_action   = ui.select(
        options=_bulk_options,
        value=_bulk_options[0],
        label='Действие для выбранных',
    ).props('dense outlined dark').style('min-width:220px; background:#1a1a1a;')

    def _refresh_sel_label(_sl=selected_names, _sn=shown_names, _lbl=sel_count_lbl):
        active = [n for n in _sl if n in _sn]
        _lbl.set_text(f'Выбрано: {len(active)} из {len(_sn)}')

    _refresh_sel_label()

    def _toggle_all(_sl=selected_names, _sn=shown_names, _rf=refresh_fn):
        if all(n in _sl for n in _sn):
            _sl.difference_update(_sn)
        else:
            _sl.update(_sn)
        _rf.refresh()

    def _apply_bulk(
        _sl=selected_names, _sn=shown_names, _dl=dismissed,
        _rf=refresh_fn, _ba=bulk_action, _pi=price_dict, _da=page_anom
    ):
        names = [n for n in _sl if n in _sn]
        if not names:
            ui.notify('Ничего не выбрано', type='warning')
            return
        label = _ba.value or '📦 Плановый приход'
        count = 0
        for n in names:
            rows = _da[_da['Наименование'] == n]
            if rows.empty:
                continue
            r = rows.iloc[0]
            try:
                _price  = float(_pi.get(n, 0) or 0)
                _impact = 0 if label in NO_IMPACT_BULK else abs(int(r['Дельта'])) * _price
            except Exception:
                _impact = 0
            db.save_anomaly_to_db({
                'item_name':        n,
                'anomaly_type':     label,
                'qty_system':       r['Стало'],
                'qty_physical':     r['Было'],
                'financial_impact': _impact,
                'source':           'Вручную (Массовое)',
                'status':           'Закрыта' if label in NO_IMPACT_BULK else 'Открыта',
                'comment':          f'Массовое действие: {label}',
            })
            _dl.append(n)
            count += 1
        _sl.difference_update(names)
        ui.notify(f'✅ {count} аномалий → {label}', type='positive')
        _rf.refresh()

    all_sel = all(n in selected_names for n in shown_names)
    with ui.row().classes('w-full items-center gap-3 flex-wrap py-2 mb-2').style(
        'background:#161616; border:1px solid #2a2a2a; border-radius:8px; padding:8px 12px;'
    ):
        ui.button(
            '☑ Снять всё' if all_sel else '☐ Выбрать все',
            on_click=_toggle_all,
        ).props('flat no-caps size=sm color=grey dense')
        sel_count_lbl
        ui.element('div').style('flex:1;')
        bulk_action
        ui.button(
            '⚡ Применить к выбранным',
            on_click=_apply_bulk,
        ).props('outline color=primary no-caps size=sm dense')

    # 6. Рисуем карточки
    for idx, row in page_anom.iterrows():
        _render_card(idx, row, df_inv, df_anomalies, expected_df, dismissed, refresh_fn,
                     price_dict=price_dict, hist_dict=hist_dict,
                     selected_names=selected_names, shown_names=shown_names,
                     sel_count_lbl=sel_count_lbl)


# ─────────────────────────────────────────────────────────────────────────────
#  Карточка одной аномалии
# ─────────────────────────────────────────────────────────────────────────────

def _render_card(idx, row, df_inv, df_anomalies, expected_df, dismissed: list, refresh_fn,
                 price_dict: dict | None = None, hist_dict: dict | None = None,
                 selected_names: set | None = None, shown_names: list | None = None,
                 sel_count_lbl=None):
    if price_dict     is None: price_dict     = {}
    if hist_dict      is None: hist_dict      = {}
    if selected_names is None: selected_names = set()
    if shown_names    is None: shown_names    = []
    status_tag, help_text, color = _get_status_tag(row)
    color_cls = _TAG_COLORS.get(color, 'text-gray-500')

    with ui.card().classes('w-full p-4 mb-4').style('background:#111111; border:1px solid #2a2a2a;'):

        # ── Заголовок ─────────────────────────────────────────────────────────
        with ui.row().classes('w-full items-start gap-4 mb-2 flex-wrap'):
            # Чекбокс массового выбора
            def _on_cb(e, _n=row['Наименование'], _sl=selected_names,
                       _sn=shown_names, _lbl=sel_count_lbl):
                if e.value:
                    _sl.add(_n)
                else:
                    _sl.discard(_n)
                if _lbl is not None:
                    active = [x for x in _sl if x in _sn]
                    _lbl.set_text(f'Выбрано: {len(active)} из {len(_sn)}')
            ui.checkbox(
                value=(row['Наименование'] in selected_names),
                on_change=_on_cb,
            ).props('color=primary dense').style('flex-shrink:0; margin-top:2px;')

            # Артикул + кнопка быстрого перехода на Склад
            with ui.column().classes('min-w-[80px]'):
                ui.label('Артикул').classes('text-xs text-gray-400 uppercase')
                _sku  = str(row.get('Артикул', '') or '').strip()
                _name = str(row.get('Наименование', ''))
                _q    = _sku if (_sku and _sku != '—') else _name[:40]
                with ui.row().classes('items-center gap-1'):
                    ui.label(_sku or '—').classes('font-mono text-sm font-semibold')
                    ui.button(
                        icon='search',
                        on_click=lambda _qv=_q: ui.navigate.to(f'/stock?q={_qv}'),
                    ).props('flat round dense size=xs color=grey').tooltip(
                        f'Открыть Склад: поиск «{_q}»'
                    )


            with ui.column().classes('flex-1'):
                ui.label(str(row['Наименование'])).classes('font-semibold text-base').style('color:white;')
                ui.label(f'{status_tag}  {help_text}').classes(f'text-xs {color_cls}')
                # ── История: последняя закрытая аномалия по этому товару ────
                _hist = hist_dict.get(row['Наименование'])
                if _hist:
                    try:
                        from datetime import datetime as _dt
                        _ts   = _dt.fromisoformat(str(_hist[0])[:19])
                        _diff = (_dt.now() - _ts).days
                        _ago  = ('сегодня' if _diff == 0 else
                                 'вчера'   if _diff == 1 else
                                 f'{_diff} дн назад')
                        _atype = str(_hist[1])
                        ui.label(f'📅 Ранее: {_atype} — {_ago}').style(
                            'color:#6b7280; font-size:0.7rem; margin-top:2px;'
                        )
                    except Exception:
                        pass
                else:
                    ui.label('✨ Первый раз в системе').style(
                        'color:#374151; font-size:0.7rem; margin-top:2px;'
                    )

            with ui.row().classes('gap-6 items-center ml-auto flex-wrap'):
                for lbl, val in [('Было', row['Было']), ('Стало', row['Стало'])]:
                    with ui.column().classes('items-center'):
                        ui.label(lbl).classes('text-xs text-gray-400 uppercase')
                        ui.label(str(val)).classes('text-sm font-semibold')
                with ui.column().classes('items-center'):
                    ui.label('Δ').classes('text-xs text-gray-400 uppercase')
                    dv  = row['Дельта']
                    cls = 'text-green-600 font-bold' if dv > 0 else 'text-red-600 font-bold'
                    ui.label(f"{'+'if dv>0 else ''}{dv}").classes(f'text-sm {cls}')
                # ── Финансовый импакт ────────────────────────────────────────
                with ui.column().classes('items-center'):
                    ui.label('≈ Сумма').classes('text-xs text-gray-400 uppercase')
                    try:
                        _price  = float(price_dict.get(row['Наименование'], 0) or 0)
                        _impact = abs(int(row['Дельта'])) * _price
                    except Exception:
                        _impact = 0
                    if _impact > 0:
                        _fmt = f'{_impact:,.0f}'.replace(',', ' ')
                        ui.label(f'≈ {_fmt} ₽').classes('text-sm text-amber-300 font-bold')
                    else:
                        ui.label('—').classes('text-sm text-gray-500')

        ui.separator()

        # ── Fuzzy Match ───────────────────────────────────────────────────────
        best_match, ratio = find_best_invoice_match(row['Наименование'], expected_df)
        if best_match is not None and ratio > 0.75:
            with ui.card().classes('w-full p-3 my-2').style('background:#0d1a0d; border:1px solid #22c55e;'):
                ui.label(
                    f"💡 Найдено в накладной ({ratio:.0%}): "
                    f"{best_match['item_name']} ({best_match['qty_expected']} шт.)"
                ).style('color:#d1fae5; font-size:0.875rem;')

                def _fuzzy_link(r=row, bm=best_match):
                    db.save_anomaly_to_db({
                        'item_name':        r['Наименование'],
                        'anomaly_type':     '📦 Плановый приход',
                        'qty_system':       r['Стало'],
                        'qty_physical':     r['Было'],
                        'financial_impact': 0,
                        'source':           'Вручную (Умная склейка накладной)',
                        'status':           'Закрыта',
                        'comment':          f"Привязка: {bm['item_name']} (id #{bm['id']})",
                    })
                    with db.get_connection() as conn:
                        conn.execute(
                            "UPDATE expected_deliveries SET status = 'Принято' WHERE id = ?",
                            (int(bm['id']),)
                        )
                        conn.commit()
                    if r['Наименование'] not in dismissed:
                        dismissed.append(r['Наименование'])
                    ui.notify('🎉 Аномалия закрыта!', type='positive')
                    refresh_fn.refresh()

                ui.button('🔗 Принять по накладной (Склеить)', on_click=_fuzzy_link).props('color=primary')

        # ── Сетка кнопок 3×3 — цвет и тултип по смысловой группе ─────────────
        NO_IMPACT     = {'Системная ошибка', '📦 Плановый приход', '⏳ Догруз с сайта'}
        NEEDS_COMMENT = {'Утеря', 'Тихая отмена', 'Пересорт (Склад)', 'Пересорт (1С)'}

        link_panel_ref: dict = {}

        def _make_handler(label, r=row, di=df_inv, lpr=link_panel_ref):
            def handler():
                # ── Открыть панель склейки ───────────────────────────────────
                if label == '🔄 Обновление карточки':
                    panel = lpr.get('panel')
                    if panel:
                        panel.set_visibility(True)
                    return

                # ── Вычислить цену заранее (нужна и для диалога и для записи)
                price = 0.0
                if not di.empty:
                    vals = di[di['Наименование'] == r['Наименование']]['Цена'].values
                    if len(vals):
                        try:
                            price = float(vals[0])
                        except Exception:
                            pass

                # ── Опасные кнопки: диалог с обязательным комментарием ──────
                if label in NEEDS_COMMENT:
                    impact     = abs(r['Дельта'] * price)
                    impact_fmt = f'{impact:,.0f}'.replace(',', ' ') if impact > 0 else None

                    with ui.dialog() as dlg:
                        with ui.card().style(
                            'background:#1a1a1a; border:1px solid #4b5563; '
                            'min-width:420px; max-width:560px; padding:20px;'
                        ):
                            # Заголовок
                            with ui.row().classes('items-center gap-2 mb-3'):
                                ui.icon('warning', size='28px').style('color:#ef4444;')
                                ui.label(f'Подтвердите: {label}').classes(
                                    'text-white font-bold text-lg'
                                )

                            # Инфо о товаре
                            ui.label(str(r['Наименование'])).classes('text-gray-300 text-sm mb-1')
                            ui.label(
                                f'Изменение: {r["Было"]} → {r["Стало"]} (Δ = {r["Дельта"]})'
                            ).classes('text-gray-400 text-xs mb-1')
                            if impact_fmt:
                                ui.label(f'Финансовый ущерб: ≈ {impact_fmt} ₽').style(
                                    'color:#fbbf24; font-size:0.85rem; margin-bottom:12px;'
                                )

                            ui.separator().style('background:#374151; margin:8px 0;')

                            # Поле комментария
                            ci = ui.input(
                                label='Комментарий (обязательно)',
                                placeholder='Укажите причину или обстоятельства...',
                            ).classes('w-full').props('outlined dark')
                            ci.style('--q-color-primary:#3b82f6;')

                            err_lbl = ui.label('').style(
                                'color:#ef4444; font-size:0.75rem; min-height:16px;'
                            )

                            def _confirm(
                                _r=r, _lbl=label, _ci=ci, _d=dlg,
                                _price=price, _el=err_lbl,
                            ):
                                comment = (_ci.value or '').strip()
                                if not comment:
                                    _el.set_text('⚠️ Комментарий не может быть пустым')
                                    return
                                _el.set_text('')
                                db.save_anomaly_to_db({
                                    'item_name':        _r['Наименование'],
                                    'anomaly_type':     _lbl,
                                    'qty_system':       _r['Стало'],
                                    'qty_physical':     _r['Было'],
                                    'financial_impact': abs(_r['Дельта'] * _price),
                                    'source':           'Вручную',
                                    'status':           'Открыта',
                                    'comment':          comment,
                                })
                                if _r['Наименование'] not in dismissed:
                                    dismissed.append(_r['Наименование'])
                                _d.close()
                                ui.notify(f'🔴 Зафиксировано: {_lbl}', type='negative', timeout=3000)
                                refresh_fn.refresh()

                            with ui.row().classes('gap-2 mt-4 w-full justify-end'):
                                ui.button('Отмена', on_click=dlg.close).props(
                                    'flat color=grey no-caps'
                                )
                                ui.button('✅ Подтвердить', on_click=_confirm).props(
                                    'color=negative no-caps'
                                )

                    dlg.open()
                    return

                # ── Безопасные кнопки: мгновенная запись ────────────────────
                auto_comment = ''
                if label == '📦 Плановый приход':
                    auto_comment = 'Штатное поступление товара'
                elif label == '⏳ Догруз с сайта':
                    auto_comment = 'Запоздалая выгрузка остатков витрины'
                db.save_anomaly_to_db({
                    'item_name':        r['Наименование'],
                    'anomaly_type':     label,
                    'qty_system':       r['Стало'],
                    'qty_physical':     r['Было'],
                    'financial_impact': abs(r['Дельта'] * price) if label not in NO_IMPACT else 0,
                    'source':           'Автоматически',
                    'status':           'Закрыта' if label in NO_IMPACT else 'Открыта',
                    'comment':          auto_comment,
                })
                if r['Наименование'] not in dismissed:
                    dismissed.append(r['Наименование'])
                ui.notify(f'Зафиксировано: {label}', type='positive')
                refresh_fn.refresh()
            return handler

        # (label, quasar-color, tooltip)
        button_defs = [
            # Ряд 1: потери / технические ошибки
            ('Утеря',              'negative', 'Товар физически отсутствует — выявлена реальная потеря. Финансовый ущерб будет записан в KPI.'),
            ('Тихая отмена',       'negative', 'Поставщик убрал позицию без уведомления. Фиксируется в KPI как потеря.'),
            ('Системная ошибка',   'grey',     'Ошибка в данных 1С или сбой синхронизации. Финансового ущерба нет — закрывается автоматически.'),
            # Ряд 2: пересорт / излишек
            ('Пересорт (Склад)',   'warning',  'Товар физически перепутан местами на складе — нужна ручная инвентаризация.'),
            ('Пересорт (1С)',      'warning',  'Позиции перепутаны в системе 1С — нужна корректировка учёта.'),
            ('Излишек',            'warning',  'Обнаружено БОЛЬШЕ товара чем числится по системе. Требует проверки.'),
            # Ряд 3: нормальные ситуации
            ('📦 Плановый приход', 'positive', 'Плановое пополнение склада по накладной. Закрывается без финансового ущерба.'),
            ('⏳ Догруз с сайта',  'positive', 'Запоздалая синхронизация остатков с витрины — норма для интернет-магазина.'),
            ('🔄 Обновление карточки', 'info', 'Товар переименован или сменился артикул. Откроет панель привязки к старой карточке.'),
        ]

        with ui.column().classes('w-full gap-2 mt-3'):
            for _row_slice in [button_defs[:3], button_defs[3:6], button_defs[6:]]:
                with ui.row().classes('w-full gap-2'):
                    for (_lbl, _col, _tip) in _row_slice:
                        ui.button(
                            _lbl,
                            on_click=_make_handler(_lbl),
                        ).classes('flex-1').props(
                            f'outline color={_col} no-caps'
                        ).tooltip(_tip)

        # ── Панель склейки ─────────────────────────────────────────────────────
        with ui.card().classes('w-full p-4 mt-3').style('background:#161616; border:1px solid #374151;') as link_panel:
            link_panel.set_visibility(False)
            link_panel_ref['panel'] = link_panel

            ui.label('🔗 Привязка к старой карточке').classes('font-semibold mb-2')

            with ui.row().classes('gap-2 mb-3'):
                def _skip(r=row):
                    db.save_anomaly_to_db({
                        'item_name':        r['Наименование'],
                        'anomaly_type':     '🔄 Обновление карточки',
                        'qty_system':       r['Стало'],
                        'qty_physical':     r['Было'],
                        'financial_impact': 0,
                        'source':           'Автоматически',
                        'status':           'Закрыта',
                        'comment':          'Изменилось название на сайте',
                    })
                    if r['Наименование'] not in dismissed:
                        dismissed.append(r['Наименование'])
                    ui.notify('Карточка обновлена без склейки', type='positive')
                    refresh_fn.refresh()

                ui.button('⏭️ Просто обновить (БЕЗ склейки)', on_click=_skip).props('color=primary outline')
                ui.button('❌ Отмена', on_click=lambda lp=link_panel: lp.set_visibility(False)).props('flat color=negative')

            search_input = ui.input(placeholder='🔍 Артикул или название...').classes('w-full')
            results_col  = ui.column().classes('w-full')

            def _do_search(value: str, r=row, da=df_anomalies, di=df_inv):
                results_col.clear()
                with results_col:
                    query = (value or '').strip()

                    if not query:
                        today_lost = (
                            da[(da['Дельта'] < 0) & (~da['Наименование'].isin(dismissed))]['Наименование'].tolist()
                            if not da.empty else []
                        )
                        if not di.empty:
                            mask = di['Наименование'].isin(today_lost) | ~di['actual']
                            matched_df = di[mask].sort_values('actual').head(10).copy()
                        else:
                            matched_df = pd.DataFrame()
                        ui.label('Показаны недавно пропавшие товары.').classes('text-xs text-gray-500 mb-1')
                    else:
                        clean = re.sub(r'\(снят с сайта.*?\)', '', query, flags=re.IGNORECASE)
                        clean = clean.replace('🔘', '').replace('❌', '').strip()
                        words = clean.lower().replace('ё', 'е').split()
                        if words and not di.empty and '_search_index' in di.columns:
                            mask = pd.Series(True, index=di.index)
                            for w in words:
                                mask &= di['_search_index'].str.contains(w, regex=False)
                            matched_df = di[mask].sort_values('actual').head(30).copy()
                            ui.label(f'🔍 Найдено: {int(mask.sum())}.').classes('text-xs text-gray-500 mb-1')
                        else:
                            matched_df = pd.DataFrame()

                    if matched_df.empty:
                        ui.label('Ничего не найдено.').classes('text-gray-400 italic')
                        return

                    with ui.row().classes('w-full text-xs text-gray-400 uppercase font-semibold px-1 mb-1'):
                        ui.label('Артикул').classes('w-24')
                        ui.label('Наименование').classes('flex-1')
                        ui.label('Статус').classes('w-32')
                        ui.label('').classes('w-28')
                    ui.separator()

                    for _, m_row in matched_df.iterrows():
                        with ui.row().classes('w-full items-center gap-2 py-1'):
                            ui.label(str(m_row.get('Артикул', '—'))).classes('font-mono text-xs w-24 truncate')
                            name_txt = str(m_row['Наименование'])
                            if not m_row.get('actual', True):
                                ui.label(f'🔘 {name_txt}').classes('flex-1 text-sm text-gray-500 truncate')
                                ui.label(f"❌ Снят ({m_row.get('last_seen_date','?')})").classes('text-xs text-red-400 w-32')
                            else:
                                ui.label(name_txt).classes('flex-1 text-sm truncate')
                                ui.label('✅ Активен').classes('text-xs text-green-600 w-32')

                            def _do_link(new_r=r, old_r=m_row):
                                old_name = old_r['Наименование']
                                with db.get_connection() as conn:
                                    conn.execute(
                                        'INSERT OR IGNORE INTO item_aliases (new_name, old_name) VALUES (?, ?)',
                                        (new_r['Наименование'], old_name)
                                    )
                                    conn.execute("""
                                        INSERT INTO anomaly_log
                                            (detected_at, item_name, anomaly_type, qty_system,
                                             qty_physical, financial_impact, source, status, comment)
                                        VALUES (datetime('now','localtime'), ?,
                                                '🔄 Обновление карточки', 0, 0, 0,
                                                'Автоматически', 'Закрыта', ?)
                                    """, (old_name, f"🔗 Склеено. Новое: {new_r['Наименование']}"))
                                    conn.commit()
                                if old_name not in dismissed:
                                    dismissed.append(old_name)
                                db.save_anomaly_to_db({
                                    'item_name':        new_r['Наименование'],
                                    'anomaly_type':     '🔄 Обновление карточки',
                                    'qty_system':       new_r['Стало'],
                                    'qty_physical':     new_r['Было'],
                                    'financial_impact': 0,
                                    'source':           'Автоматически',
                                    'status':           'Закрыта',
                                    'comment':          f'Склейка: {old_name}',
                                })
                                if new_r['Наименование'] not in dismissed:
                                    dismissed.append(new_r['Наименование'])
                                ui.notify('🔗 Карточки склеены!', type='positive')
                                refresh_fn.refresh()

                            ui.button('🔗 Склеить', on_click=_do_link).props('color=primary size=sm').classes('w-28')
                        ui.separator()

            search_input.on_value_change(lambda e: _do_search(e.value))
            # НЕ вызываем _do_search('') при рендере — только по запросу пользователя
            # чтобы не создавать тысячи UI-элементов при открытии страницы

        ui.separator().classes('mt-2')
