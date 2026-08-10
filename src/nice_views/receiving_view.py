"""
receiving_view.py — NiceGUI-версия вкладки Приёмка.
Полный перенос функционала из src/views/receiving_view.py.
Логика оцифровки дублирована из ai_services.py без зависимости от Streamlit.
"""
from nicegui import ui, run
import sys
import os
import io
import json
import base64
import logging
from pathlib import Path
import pandas as pd

_src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

import db
from nice_views.shared_layout import build_shell

logger = logging.getLogger('shadow_stock.receiving')

BASE_DIR     = Path(__file__).resolve().parent.parent.parent
SECRETS_PATH = BASE_DIR / 'src' / '.streamlit' / 'secrets.toml'
CONFIG_PATH  = BASE_DIR / 'config.json'


# ─────────────────────────────────────────────────────────────────────────────
#  Вспомогательные функции (без Streamlit)
# ─────────────────────────────────────────────────────────────────────────────

def _load_config() -> dict:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_api_key() -> str | None:
    if not SECRETS_PATH.exists():
        return None
    import tomllib
    with open(SECRETS_PATH, 'rb') as f:
        return tomllib.load(f).get('OPENROUTER_API_KEY')


def _digitize_image(image_bytes: bytes) -> list:
    """
    Отправляет фото накладной в Gemini Vision через OpenRouter.
    Возвращает список словарей: [{'название': ..., 'артикул': ..., 'количество': ...}]
    """
    import requests
    from PIL import Image

    config  = _load_config()
    api_key = _get_api_key()
    if not api_key:
        raise ValueError('OPENROUTER_API_KEY не найден в secrets.toml')

    img = Image.open(io.BytesIO(image_bytes))
    buf = io.BytesIO()
    img.convert('RGB').save(buf, format='JPEG', quality=85)
    img_b64 = base64.b64encode(buf.getvalue()).decode('utf-8')

    prompt = (
        'Ты — точный алгоритм оцифровки документов. '
        'На этой картинке таблица с товарами (накладная). '
        'ТВОЯ ЗАДАЧА: Извлечь данные из ячеек "Артикул", "Товары" и "Кол-во" СТРОГО 1 в 1. '
        'ПРАВИЛА: '
        '1. Название: Перепиши весь текст ячейки полностью. '
        '2. Артикул: Перепиши всё содержимое ячейки. '
        '3. Количество: Верни только цифру. '
        'ВЕРНИ СТРОГО МАССИВ JSON И БОЛЬШЕ НИЧЕГО. '
        'Формат: [{"название": "...", "артикул": "...", "количество": 100}]'
    )

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'https://github.com',
        'X-Title': 'Autonomous Stock Shadow',
    }
    payload = {
        'model': config['ai']['model_vision'],
        'messages': [{
            'role': 'user',
            'content': [
                {'type': 'text',      'text': prompt},
                {'type': 'image_url', 'image_url': {'url': f'data:image/jpeg;base64,{img_b64}'}},
            ],
        }],
        'temperature': config['ai']['temperature'],
    }

    resp = requests.post(
        'https://openrouter.ai/api/v1/chat/completions',
        headers=headers, json=payload, timeout=60,
    )
    resp.raise_for_status()
    raw = resp.json()['choices'][0]['message']['content']
    return json.loads(raw.replace('```json', '').replace('```', '').strip())


def _load_expected() -> pd.DataFrame:
    try:
        with db.get_connection() as conn:
            return pd.read_sql_query("""
                SELECT id, created_at, sku, item_name, qty_expected
                FROM expected_deliveries
                WHERE status = 'Ожидает'
                ORDER BY created_at DESC
            """, conn)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Страница приёмки
# ─────────────────────────────────────────────────────────────────────────────

def setup_page():

    @ui.page('/receiving')
    def receiving_page():
        logger.info('receiving_page() handler entered')
        build_shell('/receiving')

        image_bytes_ref:  list = [None]
        temp_invoice_ref: list = [None]

        with ui.column().classes('w-full p-4 gap-6').style(
            'background:#0d0d0d; min-height:100vh;'
        ):
            # ══════════════════════════════════════════════════════════════
            # СЕКЦИЯ 1: Оцифровка накладной
            # ══════════════════════════════════════════════════════════════
            with ui.card().classes('w-full p-6').style(
                'background:#111111; border:1px solid #2a2a2a;'
            ):
                ui.label('📸 Оцифровка накладной (Нейро-приёмка)').classes(
                    'text-white text-xl font-bold'
                )
                ui.label(
                    'Загрузите фото таблицы с товарами. '
                    'Цены и контрагентов в кадр брать не нужно.'
                ).style('color:#9ca3af; font-size:0.85rem;')
                ui.separator().style('background:#2a2a2a; margin:12px 0;')

                def on_upload(e):
                    raw = e.content.read()
                    image_bytes_ref[0] = raw

                    from PIL import Image as _PIL
                    img = _PIL.open(io.BytesIO(raw))
                    buf = io.BytesIO()
                    img.convert('RGB').save(buf, format='JPEG', quality=80)
                    b64 = base64.b64encode(buf.getvalue()).decode()

                    preview_area.clear()
                    with preview_area:
                        ui.image(f'data:image/jpeg;base64,{b64}') \
                          .classes('rounded-lg') \
                          .style('max-width:420px; border:1px solid #2a2a2a;')
                        ui.label(f'📸 {e.name}').style('color:#9ca3af; font-size:0.8rem;')

                    btn_area.clear()
                    with btn_area:
                        status_lbl = ui.label('').style('color:#818cf8; font-weight:600;')
                        status_lbl.set_visibility(False)

                        async def do_digitize(_btn=None, _lbl=status_lbl):
                            _btn.set_enabled(False)
                            _lbl.set_text('🧠 Нейросеть Gemini читает таблицу…')
                            _lbl.set_visibility(True)
                            try:
                                items = await run.io_bound(
                                    _digitize_image, image_bytes_ref[0]
                                )
                                temp_invoice_ref[0] = items
                                refresh_result()
                                ui.notify(
                                    f'✅ Распознано позиций: {len(items)}',
                                    type='positive',
                                )
                            except Exception as ex:
                                logger.exception('Ошибка оцифровки накладной')
                                ui.notify(
                                    f'❌ Ошибка распознавания: {ex}',
                                    type='negative', timeout=0,
                                )
                            finally:
                                _btn.set_enabled(True)
                                _lbl.set_visibility(False)

                        digi_btn = ui.button(
                            '🚀 Отправить на оцифровку',
                        ).props('color=primary').classes('w-full')
                        digi_btn.on_click(
                            lambda: do_digitize(_btn=digi_btn, _lbl=status_lbl)
                        )

                ui.upload(
                    label='📂 Выберите фото из галереи (накладная):',
                    on_upload=on_upload,
                    max_file_size=10_000_000,
                ).props("accept='.jpg,.jpeg,.png' flat").classes('w-full')

                preview_area = ui.column().classes('w-full items-center gap-2')
                btn_area     = ui.column().classes('w-full gap-2')
                result_area  = ui.column().classes('w-full gap-4')

                def refresh_result():
                    result_area.clear()
                    if not temp_invoice_ref[0]:
                        return
                    with result_area:
                        ui.separator().style('background:#2a2a2a;')
                        items = temp_invoice_ref[0]

                        # ── Нормализуем типы (qty → int) и считаем предупреждения ──
                        rows: list[dict] = []
                        for it in items:
                            try:
                                qty = int(it.get('количество', 0) or 0)
                            except (ValueError, TypeError):
                                qty = 0
                            rows.append({
                                'название':    str(it.get('название',  '') or '').strip(),
                                'артикул':     str(it.get('артикул',   '') or '').strip(),
                                'количество':  qty,
                            })

                        zero_qty = sum(1 for r in rows if r['количество'] == 0)
                        no_name  = sum(1 for r in rows if not r['название'])

                        # ── Шапка результата ──────────────────────────────────────
                        with ui.row().classes('w-full items-center justify-between flex-wrap gap-2'):
                            ui.label(f'✅ Распознано: {len(rows)} позиций').classes(
                                'text-green-400 font-semibold text-base'
                            )
                            if zero_qty:
                                ui.label(f'⚠️ {zero_qty} строк с нулевым кол-вом').style(
                                    'color:#fbbf24; font-size:0.82rem;'
                                )
                            if no_name:
                                ui.label(f'🔴 {no_name} строк без названия (пропустятся)').style(
                                    'color:#ef4444; font-size:0.82rem;'
                                )

                        ui.label(
                            '✏️ Нажмите на ячейку чтобы отредактировать. '
                            'Строки с нулевым количеством подсвечены красным.'
                        ).style('color:#6b7280; font-size:0.78rem; margin:4px 0 6px;')

                        # ── Редактируемый aggrid ──────────────────────────────────
                        grid = ui.aggrid({
                            'columnDefs': [
                                {
                                    'field':       'название',
                                    'headerName':  'Наименование',
                                    'editable':    True,
                                    'flex':        1,
                                    'minWidth':    200,
                                    'cellEditor':  'agTextCellEditor',
                                    'cellStyle':   {'color': 'white'},
                                },
                                {
                                    'field':       'артикул',
                                    'headerName':  'Артикул',
                                    'editable':    True,
                                    'width':       150,
                                    'cellEditor':  'agTextCellEditor',
                                    'cellStyle':   {
                                        'color':      '#a5b4fc',
                                        'fontFamily': 'monospace',
                                    },
                                },
                                {
                                    'field':       'количество',
                                    'headerName':  'Кол-во',
                                    'editable':    True,
                                    'width':       110,
                                    'type':        'numericColumn',
                                    'cellEditor':  'agNumberCellEditor',
                                    # красный если 0, зелёный если > 0
                                    'cellStyle':   {
                                        'function': (
                                            'params.value == 0 '
                                            '? {color:"#ef4444", fontWeight:"bold"} '
                                            ': {color:"#34d399", fontWeight:"bold"}'
                                        )
                                    },
                                },
                            ],
                            'rowData':   rows,
                            'domLayout': 'autoHeight',
                            'stopEditingWhenCellsLoseFocus': True,
                            # строка красного фона если qty == 0
                            'getRowStyle': {
                                'function': (
                                    'params.data.количество == 0 '
                                    '? {background:"#2d1111"} : null'
                                )
                            },
                        }).classes('w-full ag-theme-balham-dark')

                        grid_ref: list = [grid]

                        # ── Сохранение: читаем АКТУАЛЬНЫЕ данные из браузера ─────
                        async def save_invoice(_gr=grid_ref, _tir=temp_invoice_ref):
                            edited = await _gr[0].get_client_data()
                            if not edited:
                                ui.notify('Нет данных для сохранения', type='warning')
                                return

                            valid   = [r for r in edited if str(r.get('название', '')).strip()]
                            skipped = len(edited) - len(valid)

                            with db.get_connection() as conn:
                                for item in valid:
                                    try:
                                        qty = int(item.get('количество', 0) or 0)
                                    except (ValueError, TypeError):
                                        qty = 0
                                    conn.execute("""
                                        INSERT INTO expected_deliveries
                                            (item_name, sku, qty_expected)
                                        VALUES (?, ?, ?)
                                    """, (
                                        str(item.get('название', '')).strip(),
                                        str(item.get('артикул',  '') or '').strip(),
                                        qty,
                                    ))
                                conn.commit()

                            _tir[0] = None
                            refresh_result()
                            render_expected.refresh()

                            msg = f'🎉 Добавлено {len(valid)} позиций!'
                            if skipped:
                                msg += f'  (пропущено {skipped} без названия)'
                            ui.notify(msg, type='positive')

                        def cancel_result(_tir=temp_invoice_ref):
                            _tir[0] = None
                            refresh_result()

                        with ui.row().classes('w-full gap-2 mt-3'):
                            ui.button(
                                '💾 Подтвердить и сохранить',
                                on_click=save_invoice,
                            ).props('color=primary no-caps').classes('flex-1')
                            ui.button(
                                '✕ Отменить',
                                on_click=cancel_result,
                            ).props('flat color=negative no-caps')


            ui.separator().style('background:#2a2a2a;')

            # ══════════════════════════════════════════════════════════════
            # СЕКЦИЯ 2: Список ожидаемых товаров
            # ══════════════════════════════════════════════════════════════
            with ui.row().classes('w-full items-center justify-between flex-wrap gap-2'):
                ui.label('📋 Список ожидаемых товаров').classes('text-white text-xl font-bold')

                def clear_all():
                    with db.get_connection() as conn:
                        conn.execute(
                            "DELETE FROM expected_deliveries WHERE status = 'Ожидает'"
                        )
                        conn.commit()
                    ui.notify('✅ Список очищен!', type='positive')
                    render_expected.refresh()

                ui.button('🗑️ Очистить весь список', on_click=clear_all) \
                  .props('flat color=negative no-caps')

            ui.label(
                'Эти позиции оцифрованы и ждут появления на сайте '
                'для авто-легализации аномалий.'
            ).style('color:#9ca3af; font-size:0.85rem; margin-top:-12px;')

            @ui.refreshable
            def render_expected():
                expected_df = _load_expected()

                if expected_df.empty:
                    with ui.card().classes('w-full p-4').style(
                        'background:#111111; border:1px solid #2a2a2a;'
                    ):
                        ui.label('ℹ️ В листе ожидания пока ничего нет.').classes('text-gray-400')
                    return

                with ui.card().classes('w-full').style(
                    'background:#111111; border:1px solid #2a2a2a; overflow:hidden;'
                ):
                    with ui.row().classes('w-full items-center gap-2 px-4 py-2').style(
                        'background:#1a1a1a; border-bottom:1px solid #2a2a2a;'
                    ):
                        for lbl, w in [
                            ('Дата', '130px'), ('Артикул', '110px'),
                            ('Наименование', None), ('Ожидаем', '90px'), ('', '44px'),
                        ]:
                            ui.label(lbl).style(
                                f'color:#6b7280; font-size:0.7rem; font-weight:700; '
                                f'text-transform:uppercase; letter-spacing:0.05em; '
                                f'{"flex:1;" if w is None else f"min-width:{w}; flex-shrink:0;"}'
                            )

                    for _, row in expected_df.iterrows():
                        with ui.row().classes('w-full items-center gap-2 px-4 py-2').style(
                            'border-bottom:1px solid #1a1a1a;'
                        ):
                            ui.label(str(row['created_at'])[:16]).style(
                                'color:#9ca3af; font-size:0.8rem; min-width:130px; flex-shrink:0;'
                            )
                            sku_txt = (
                                str(row['sku'])
                                if pd.notna(row['sku']) and row['sku'] else '—'
                            )
                            ui.label(sku_txt).classes('font-mono text-sm text-white').style(
                                'min-width:110px; flex-shrink:0;'
                            )
                            ui.label(str(row['item_name'])).classes('flex-1 text-sm text-white')
                            ui.label(f"{row['qty_expected']} шт.").style(
                                'color:#34d399; min-width:90px; flex-shrink:0; text-align:right;'
                            )

                            def _delete(
                                _id=int(row['id']),
                                _name=str(row['item_name'])
                            ):
                                with db.get_connection() as conn:
                                    conn.execute(
                                        'DELETE FROM expected_deliveries WHERE id = ?', (_id,)
                                    )
                                    conn.commit()
                                ui.notify(f'🗑️ Удалён: {_name}', type='info')
                                render_expected.refresh()

                            ui.button('❌', on_click=_delete) \
                              .props('flat size=sm color=negative') \
                              .tooltip('Удалить из листа ожидания') \
                              .style('min-width:44px; flex-shrink:0;')

            render_expected()
