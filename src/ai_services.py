import os
import json
import time
import logging
import sqlite3
import statistics
import tomllib
import pandas as pd
import requests
import base64
import io
from pathlib import Path
from PIL import Image

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.json"
SECRETS_PATH = BASE_DIR / "src" / ".streamlit" / "secrets.toml"

def load_config() -> dict:
    """Загружает конфигурацию из config.json"""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Конфигурационный файл не найден: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

def get_api_key():
    if not SECRETS_PATH.exists(): return None
    with open(SECRETS_PATH, "rb") as f:
        return tomllib.load(f).get("OPENROUTER_API_KEY")


def call_openrouter(payload: dict, max_attempts: int = 3) -> str:
    """
    POST-запрос к OpenRouter с retry и exponential backoff.
    max_attempts: сколько раз пытаться (2s / 4s / 8s паузы между попытками).
    Возвращает текст ответа LLM или поднимает исключение с подробным описанием.
    """
    api_key = get_api_key()
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY не найден в secrets.toml")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
        "HTTP-Referer":  "https://github.com",
        "X-Title":       "Autonomous Stock Shadow",
    }

    last_exc: Exception = RuntimeError("Нет попыток")
    for attempt in range(max_attempts):
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60,
            )
            if response.status_code != 200:
                body = response.text[:300]
                raise requests.HTTPError(
                    f"HTTP {response.status_code}: {body}",
                    response=response,
                )
            return response.json()["choices"][0]["message"]["content"]

        except Exception as exc:
            last_exc = exc
            wait_sec = 2 ** (attempt + 1)          # 2 → 4 → 8 сек
            logging.warning(
                f"[OpenRouter] Попытка {attempt + 1}/{max_attempts} не удалась: {exc}. "
                + (f"Повтор через {wait_sec}с..." if attempt < max_attempts - 1 else "Попытки исчерпаны.")
            )
            if attempt < max_attempts - 1:
                time.sleep(wait_sec)

    _log_llm_error(str(last_exc))
    raise last_exc


def _log_llm_error(message: str) -> None:
    """
    Пишет ошибку LLM в отдельный лог-файл (logs/llm_errors.log).
    ab_test_view читает этот файл чтобы показать уведомление пользователю.
    """
    try:
        log_path = BASE_DIR / "logs" / "llm_errors.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(f"{ts} | {message}\n")
    except Exception:
        pass  # не даём ошибке логирования сломать основной поток


# ==========================================
# АГЕНТ 1: ОЦИФРОВКА НАКЛАДНЫХ (VISION)
# ==========================================
def digitize_invoice(image_file) -> list:
    img = Image.open(image_file)
    buffered = io.BytesIO()
    img.convert('RGB').save(buffered, format="JPEG", quality=85)
    img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
    
    prompt = """
    Ты — точный алгоритм оцифровки документов. 
    На этой картинке таблица с товарами (накладная). 
    ТВОЯ ЗАДАЧА: Извлечь данные из ячеек "Артикул", "Товары" и "Кол-во" СТРОГО 1 в 1.
    ПРАВИЛА:
    1. Название: Перепиши весь текст ячейки полностью.
    2. Артикул: Перепиши всё содержимое ячейки.
    3. Количество: Верни только цифру.
    ВЕРНИ СТРОГО МАССИВ JSON И БОЛЬШЕ НИЧЕГО. 
    Формат: [{"название": "...", "артикул": "...", "количество": 100}]
    """
    
    payload = {
        "model": CONFIG['ai']['model_vision'],
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_str}"}}
                ]
            }
        ],
        "temperature": CONFIG['ai']['temperature']
    }
    
    raw_text = call_openrouter(payload)
    return json.loads(raw_text.replace("```json", "").replace("```", "").strip())

# ==========================================
# АГЕНТ 2: ПРОГНОЗ ОСАТКОВ (FORECASTING)
# ==========================================
def run_batch_forecast():
    if not get_api_key(): return "no_key"

    db_path = BASE_DIR / CONFIG['paths']['data_dir'] / CONFIG['paths']['db_name']
    with sqlite3.connect(db_path) as conn:
        history_days = CONFIG['ai']['forecast_history_days']
        items_limit = CONFIG['ai']['forecast_items_limit']
        
        active_items = pd.read_sql_query(f"""
            SELECT 
                item_name, sku,
                MAX(quantity) as peak_qty,
                (SELECT quantity FROM stocks s2 WHERE s2.item_name = s.item_name ORDER BY report_timestamp DESC LIMIT 1) as current_qty,
                (SELECT price FROM stocks s3 WHERE s3.item_name = s.item_name ORDER BY report_timestamp DESC LIMIT 1) as price
            FROM stocks s
            WHERE report_timestamp >= date('now', '-{history_days} days', 'localtime')
            GROUP BY item_name
            HAVING current_qty < peak_qty AND current_qty > 0
            ORDER BY (peak_qty - current_qty) DESC
            LIMIT {items_limit}
        """, conn)

        if active_items.empty: return "empty"

        # ── Батч-загрузка ожидаемых поставок для всех товаров одним запросом ──
        # Учитываем только записи со статусом 'Ожидает' (ещё не поступили на склад).
        # Результат: словарь {item_name: qty_in_transit}
        all_names = active_items['item_name'].tolist()
        placeholders = ','.join('?' * len(all_names))
        try:
            deliveries_df = pd.read_sql_query(f"""
                SELECT item_name, SUM(qty_expected) AS qty_in_transit
                FROM expected_deliveries
                WHERE status = 'Ожидает'
                  AND item_name IN ({placeholders})
                GROUP BY item_name
            """, conn, params=all_names)
            in_transit_map = dict(
                zip(deliveries_df['item_name'], deliveries_df['qty_in_transit'])
            )
        except Exception:
            # Таблица может не существовать если Приёмка ещё не использовалась
            in_transit_map = {}

        batch_size = CONFIG['ai']['forecast_batch_size']
        success_count = 0

        # ── ABC-анализ до разбивки на подбатчи ────────────────────────────
        # Оборот товара = avg_sales × price за период наблюдения
        # Группы:
        #   A — товары дающие первые 80% оборота (критически важные)
        #   B — 80–95%  (умеренно важные)
        #   C — 95–100% (низкоприоритетные)
        abc_df = active_items.copy()
        abc_df['revenue'] = (
            pd.to_numeric(abc_df.get('price', 0), errors='coerce').fillna(0)
            * pd.to_numeric(abc_df['peak_qty'] - abc_df['current_qty'], errors='coerce').fillna(0)
        ).clip(lower=0)
        # Если цены нет — фоллбек: используем объём продаж как прокси выручки
        if abc_df['revenue'].sum() == 0:
            abc_df['revenue'] = (
                pd.to_numeric(abc_df['peak_qty'] - abc_df['current_qty'], errors='coerce').fillna(0)
            ).clip(lower=0)
        total_rev = abc_df['revenue'].sum()
        abc_df = abc_df.sort_values('revenue', ascending=False)
        abc_df['cum_share'] = abc_df['revenue'].cumsum() / total_rev if total_rev > 0 else 0
        abc_df['abc'] = 'C'
        abc_df.loc[abc_df['cum_share'] <= 0.95, 'abc'] = 'B'
        abc_df.loc[abc_df['cum_share'] <= 0.80, 'abc'] = 'A'
        abc_map = dict(zip(abc_df['item_name'], abc_df['abc']))

        # Обеспечиваем наличие колонки abc_category в БД (backward-compatible)
        try:
            conn.execute("ALTER TABLE ai_forecasts ADD COLUMN abc_category TEXT DEFAULT 'C'")
            conn.commit()
        except Exception:
            pass  # колонка уже есть

        for i in range(0, len(active_items), batch_size):
            batch = active_items.iloc[i:i+batch_size]
            items_data = []
            for _, row in batch.iterrows():
                # Берём последний остаток за каждый день (дедуплицируем внутридневные записи)
                df_hist = pd.read_sql_query(f"""
                    SELECT
                        SUBSTR(report_timestamp, 1, 10) AS date,
                        quantity
                    FROM stocks
                    WHERE item_name = ?
                      AND report_timestamp >= date('now', '-{history_days} days')
                    GROUP BY SUBSTR(report_timestamp, 1, 10)
                    HAVING report_timestamp = MAX(report_timestamp)
                    ORDER BY date ASC
                """, conn, params=(row['item_name'],))

                # --- КОРРЕКТНЫЙ РАСЧЁТ avg_sales ЧЕРЕЗ ДНЕВНЫЕ ДЕЛЬТЫ ---
                # Логика: если остаток уменьшился с вчера на сегодня — это продажи.
                #         если остаток увеличился — это поставка (не считаем как продажи).
                # Это корректно отделяет продажи от приходов товара.
                if len(df_hist) > 1:
                    quantities = df_hist['quantity'].tolist()
                    daily_sales_list = []
                    for j in range(1, len(quantities)):
                        delta = quantities[j] - quantities[j - 1]
                        if delta < 0:
                            # Остаток уменьшился — это продажи
                            daily_sales_list.append(abs(delta))
                        # delta > 0 → поставка, пропускаем
                        # delta == 0 → нет движения, пропускаем

                    if daily_sales_list:
                        # Среднее по дням с продажами, но делим на все дни (включая нулевые)
                        # чтобы учитывать дни без движения товара
                        days_tracked = max(1, len(df_hist) - 1)
                        avg_sales = round(sum(daily_sales_list) / days_tracked, 2)
                    else:
                        # Нет ни одного дня со снижением остатка — скорее всего новый товар
                        avg_sales = 0.0
                else:
                    # Только одна точка данных — не можем считать дельты
                    avg_sales = 0.0
                    days_tracked = 1
                
                # --- МАТЕМАТИЧЕСКИЙ РАСЧЁТ ПРОГНОЗА ---
                current_qty    = int(row['current_qty'])
                qty_in_transit = int(in_transit_map.get(row['item_name'], 0))
                lead_time      = CONFIG['ai']['lead_time_days']
                z              = CONFIG['ai']['safety_stock_multiplier']

                # std_dev считаем по дневным продажам (не по остаткам!).
                # Std по остаткам раздувается из-за поставок и даёт неверный страховой запас.
                if len(daily_sales_list) >= 2:
                    std_dev = statistics.stdev(daily_sales_list)
                elif len(daily_sales_list) == 1:
                    std_dev = daily_sales_list[0] * 0.2  # условная волатильность 20%
                else:
                    std_dev = 0.0

                # Fallback: если данных мало, используем 20% от среднего расхода
                if len(daily_sales_list) < 3 or std_dev == 0:
                    safety_stock = int(avg_sales * 0.2)
                else:
                    # Страховой запас: z × σ × sqrt(lead_time)
                    safety_stock = int(z * std_dev * (lead_time ** 0.5))

                # ── Стандартная формула закупок (ROP — Reorder Point) ──────────
                #
                # ROP = avg_sales × lead_time + safety_stock
                #
                # Сколько заказать = ROP − (текущий_остаток + в_пути).
                # qty_in_transit — товар из expected_deliveries (статус 'Ожидает'):
                # он ещё не на складе, но уже оплачен и едет, поэтому вычитаем его
                # из потребности чтобы не заказывать то, что уже в дороге.
                #
                # Пример: avg=10/д, lead=14д, safety=15, остаток=50, в_пути=60
                #   ROP   = 10×14 + 15 = 155
                #   order = max(0, 155 − 50 − 60) = 45 шт  (без учёта: 105 шт)

                reorder_point  = int(avg_sales * lead_time) + safety_stock
                effective_stock = current_qty + qty_in_transit
                recommended_qty = max(0, reorder_point - effective_stock)

                # Дни до нуля — считаем только по текущему остатку
                # (поставка ещё не пришла, неизвестно когда придёт)
                days_to_zero = round(current_qty / avg_sales, 1) if avg_sales > 0 else 999.0

                items_data.append({
                    "name":           row['item_name'],
                    "sku":            row['sku'],
                    "stock":          current_qty,
                    "in_transit":     qty_in_transit,
                    "avg_sales":      avg_sales,
                    "lead_time":      lead_time,
                    "safety_stock":   safety_stock,
                    "reorder_point":  reorder_point,
                    "recommended_qty": recommended_qty,
                    "days_to_zero":   days_to_zero,
                    "abc":            abc_map.get(row['item_name'], 'C'),
                })

            today_date = pd.Timestamp.now().strftime('%Y-%m-%d')

            # ── Вспомогательная функция: детерминированный диагноз ───────────
            # urgency, risk, action вычисляет Python (не LLM) — предсказуемо и быстро.
            # LLM добавляет только 'note' — контекст который Python не знает.
            def _build_diagnosis(item: dict, note: str = '') -> str:
                dtoz = item['days_to_zero']
                lt   = item['lead_time']
                rop  = int(item['avg_sales'] * item['lead_time']) + item['safety_stock']
                eff  = item['stock'] + item.get('in_transit', 0)

                if dtoz < lt:
                    urgency = '🔴 Критично'
                    risk    = (
                        f"Остаток обнулится через {dtoz:.0f} дн., "
                        f"а поставка идёт {lt} дн. — товар закончится до прихода."
                    )
                elif dtoz < lt * 1.5:
                    urgency = '🟡 Внимание'
                    risk    = (
                        f"Хватит на {dtoz:.0f} дн. при сроке поставки {lt} дн. "
                        f"— запас критически мал."
                    )
                else:
                    urgency = '🟢 Норма'
                    risk    = f"Хватит на {dtoz:.0f} дн. — запаса достаточно для покрытия поставки."

                action = f"Заказать {item['recommended_qty']} шт (ROP {rop} − склад+пути {eff})."

                parts = [urgency, f"Риск: {risk}", f"Действие: {action}"]
                if note:
                    parts.append(f"💡 {note}")
                return ' | '.join(parts)

            # ── Промпт для LLM: только поле 'note' ───────────────────────────
            # Передаём краткую сводку без избыточных цифр.
            # LLM видит: название, ABC, дней до нуля, в пути — и пишет один
            # нетривиальный комментарий (сезон, тренд, аномалия, риск категории).
            llm_summaries = [
                {
                    "name":        item["name"],
                    "abc":         item["abc"],
                    "days_to_zero": item["days_to_zero"],
                    "avg_sales":   item["avg_sales"],
                    "in_transit":  item.get("in_transit", 0),
                    "recommended_qty": item["recommended_qty"],
                }
                for item in items_data
            ]
            prompt = (
                f"Сегодня: {today_date}.\n"
                f"Данные по товарам:\n{json.dumps(llm_summaries, ensure_ascii=False, indent=2)}\n\n"
                "Для каждого товара напиши ОДНО предложение-примечание (note) — "
                "что-то нетривиальное, что не видно из цифр: сезонность, "
                "необычный темп продаж, риск категории A, влияние in_transit и т.п. "
                "Если нечего добавить — верни пустую строку.\n"
                "ВЕРНИ СТРОГО JSON-массив (без markdown):\n"
                '[ {"item_name": "...", "sku": "...", "note": "..."} ]\n'
                "Одно предложение на товар. Кратко. На русском."
            )
            payload = {
                "model":       CONFIG['ai']['model_forecast'],
                "messages":    [{"role": "user", "content": prompt}],
                "temperature": CONFIG['ai']['temperature'],
            }

            retry_count = CONFIG['crawler'].get('retry_count', 3)
            note_map: dict = {}   # {item_name: note_text}
            llm_used = False

            try:
                raw_text  = call_openrouter(payload, max_attempts=retry_count)
                llm_notes = json.loads(
                    raw_text.replace("```json", "").replace("```", "").strip()
                )
                if not isinstance(llm_notes, list) or not llm_notes:
                    raise ValueError('LLM вернул пустой или некорректный список')
                if 'note' not in llm_notes[0] and 'item_name' not in llm_notes[0]:
                    raise ValueError('LLM не вернул поле note')
                note_map = {r.get('item_name', ''): r.get('note', '') for r in llm_notes}
                llm_used = True

            except Exception as llm_exc:
                _log_llm_error(f"[run_batch_forecast] {llm_exc}")
                logging.warning(
                    f"[AI] LLM недоступен: {llm_exc}. "
                    f"Диагноз будет без note для {len(items_data)} товаров."
                )

            # ── Собираем финальные записи для БД ─────────────────────────────
            forecasts = [
                {
                    "item_name": item["name"],
                    "sku":       item["sku"],
                    "reason":    _build_diagnosis(item, note_map.get(item["name"], "")),
                }
                for item in items_data
            ]

            # ── Запись в БД (один раз за батч, после получения forecasts) ────────
            try:
                for f in forecasts:
                    item_data = next(
                        (item for item in items_data if item['name'] == f['item_name']),
                        None
                    )
                    if not item_data:
                        continue

                    avg_s = item_data['avg_sales']
                    days_to_zero = item_data['days_to_zero']
                    calc_zero_date = (
                        pd.Timestamp.now() + pd.Timedelta(days=int(days_to_zero))
                    ).strftime('%Y-%m-%d')

                    existing = conn.execute(
                        "SELECT id FROM ai_forecasts "
                        "WHERE item_name = ? AND date(created_at) = date('now', 'localtime')",
                        (f['item_name'],)
                    ).fetchone()

                    if existing:
                        conn.execute("""
                            UPDATE ai_forecasts
                            SET predicted_zero_date = ?, recommended_qty = ?, reason = ?,
                                avg_daily_sales = ?, lead_time_days = ?, safety_stock = ?,
                                base_demand = ?, abc_category = ?, status = '⏳ Наблюдение'
                            WHERE id = ?
                        """, (
                            calc_zero_date, item_data['recommended_qty'], f['reason'],
                            avg_s, item_data['lead_time'], item_data['safety_stock'],
                            item_data['reorder_point'], item_data['abc'], existing[0]
                        ))
                    else:
                        conn.execute(
                            "UPDATE ai_forecasts SET status = '🔄 Пересчитан ИИ' "
                            "WHERE item_name = ? AND status = '⏳ Наблюдение'",
                            (f['item_name'],)
                        )
                        conn.execute("""
                            INSERT INTO ai_forecasts
                            (item_name, sku, predicted_zero_date, recommended_qty, reason,
                             avg_daily_sales, lead_time_days, safety_stock, base_demand, abc_category)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            f['item_name'], f['sku'], calc_zero_date,
                            item_data['recommended_qty'], f['reason'],
                            avg_s, item_data['lead_time'], item_data['safety_stock'],
                            item_data['reorder_point'], item_data['abc']
                        ))

                conn.commit()
                success_count += len(forecasts)
                logging.info(
                    f"[AI] Батч сохранён: {len(forecasts)} прогнозов "
                    f"({'LLM' if llm_used else 'шаблон'})."
                )

            except Exception as db_err:
                logging.error(f"[AI] Ошибка записи в БД: {db_err}")
                return f"error_{db_err}"

            time.sleep(2)

    return f"ok_{success_count}"



# ─────────────────────────────────────────────────────────────────────────────
#  Точка входа для запуска как скрипта (python src/ai_services.py)
#  Вызывается из autostart.py после парсинга. Можно запустить вручную.
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    import sys
    from pathlib import Path as _Path

    _log_dir = BASE_DIR / 'logs'
    _log_dir.mkdir(parents=True, exist_ok=True)
    _fh = logging.FileHandler(_log_dir / 'ai_forecaster.log', encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logging.getLogger().addHandler(_fh)

    logging.info('=== AI Forecaster (ai_services.py) запущен ===')

    _pending_path = None
    try:
        _pending_path = BASE_DIR / CONFIG['paths']['ai_pending_flag']
    except Exception:
        pass

    _result = run_batch_forecast()
    logging.info(f'run_batch_forecast() -> {_result}')

    if _result == 'no_key':
        logging.error('API-ключ не найден.')
        sys.exit(1)
    elif _result == 'empty':
        logging.warning('Нет товаров для прогноза.')
        if _pending_path and _pending_path.exists():
            _pending_path.unlink()
        sys.exit(0)
    elif isinstance(_result, str) and _result.startswith('error_'):
        logging.error(f'Ошибка: {_result.split("_", 1)[1]}')
        sys.exit(1)
    elif isinstance(_result, str) and _result.startswith('ok_'):
        logging.info(f'Готово. Прогнозов: {_result.split("_", 1)[1]}.')
        if _pending_path and _pending_path.exists():
            _pending_path.unlink()
        sys.exit(0)
    else:
        logging.warning(f'Неожиданный результат: {_result}')
        sys.exit(0)

