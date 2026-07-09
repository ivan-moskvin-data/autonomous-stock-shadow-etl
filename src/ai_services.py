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
import streamlit as st

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

@st.cache_data(ttl=60, show_spinner=False)
def check_ai_connection() -> bool:
    """Проверяет доступность OpenRouter (работает без прокси)"""
    try:
        requests.get("https://openrouter.ai/api/v1/models", timeout=3.0)
        return True
    except:
        return False

def call_openrouter(payload: dict) -> str:
    api_key = get_api_key()
    if not api_key: raise ValueError("OPENROUTER_API_KEY не найден в secrets.toml")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com",
        "X-Title": "Autonomous Stock Shadow"
    }
    
    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers=headers,
        json=payload,
        timeout=60
    )
    response.raise_for_status()
    return response.json()['choices'][0]['message']['content']

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

        batch_size = CONFIG['ai']['forecast_batch_size']
        success_count = 0
        
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
                current_qty = int(row['current_qty'])
                lead_time = CONFIG['ai']['lead_time_days']
                z = CONFIG['ai']['safety_stock_multiplier']
                
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
                # ROP = сколько товара нужно иметь в момент размещения заказа,
                #       чтобы покрыть потребность за время поставки + страховой запас.
                #
                #   ROP = avg_sales × lead_time + safety_stock
                #
                # Сколько заказать = ROP - текущий_остаток (если остаток ниже ROP).
                # Если текущий остаток уже выше ROP — заказывать не нужно (= 0).
                #
                # Пример: avg=10/день, lead=14дн, safety=15, остаток=50
                #   ROP = 10×14 + 15 = 155
                #   order = 155 - 50 = 105 шт  ← корректный заказ
                #
                # Старая формула давала: 50 + 10×14 + 15 = 205 шт  ← завышена вдвое

                reorder_point = int(avg_sales * lead_time) + safety_stock
                recommended_qty = max(0, reorder_point - current_qty)
                
                # Дни до нуля (математически, без LLM)
                days_to_zero = round(current_qty / avg_sales, 1) if avg_sales > 0 else 999.0
                
                items_data.append({
                    "name": row['item_name'],
                    "sku": row['sku'],
                    "stock": current_qty,
                    "avg_sales": avg_sales,
                    "lead_time": lead_time,
                    "safety_stock": safety_stock,
                    "reorder_point": reorder_point,
                    "recommended_qty": recommended_qty,
                    "days_to_zero": days_to_zero
                })

            today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            # LLM используется только для генерации текстового обоснования (reason).
            # Вся математика (avg_sales, ROP, recommended_qty) уже посчитана выше.
            prompt = (
                f"Сегодня: {today_date}. ДАННЫЕ: {json.dumps(items_data, ensure_ascii=False)}. "
                f"ПРАВИЛА: 1. 'reason' — краткое обоснование прогноза на основе математических расчётов. "
                f"ВЕРНИ JSON: [ {{\"item_name\": \"...\", \"sku\": \"...\", \"reason\": \"...\"}} ]"
            )
            payload = {
                "model": CONFIG['ai']['model_forecast'],
                "messages": [{"role": "user", "content": prompt}],
                "temperature": CONFIG['ai']['temperature'],
            }

            retry_count = CONFIG['crawler'].get('retry_count', 3)
            forecasts = None
            llm_used = False

            for attempt in range(retry_count):
                try:
                    raw_text = call_openrouter(payload)
                    forecasts = json.loads(
                        raw_text.replace("```json", "").replace("```", "").strip()
                    )
                    llm_used = True
                    break  # ← только здесь, после успешного парсинга ответа LLM

                except Exception as e:
                    wait_sec = 2 ** (attempt + 1)  # 2, 4, 8 секунд
                    logging.warning(
                        f"[AI] Попытка {attempt + 1}/{retry_count} не удалась: {e}. "
                        f"Повтор через {wait_sec}с..."
                    )
                    if attempt < retry_count - 1:
                        time.sleep(wait_sec)

            if forecasts is None:
                # Все попытки исчерпаны — используем шаблонный reason, но логируем
                logging.warning(
                    f"[AI] LLM недоступен после {retry_count} попыток. "
                    f"Используем шаблонный reason для {len(items_data)} товаров."
                )
                forecasts = []
                for item in items_data:
                    rop = int(item['avg_sales'] * item['lead_time']) + item['safety_stock']
                    reason = (
                        f"[Авто] Расход: {item['avg_sales']:.2f} шт/день. "
                        f"Хватит на: {item['days_to_zero']:.1f} дней. "
                        f"ROP = {int(item['avg_sales'])} × {item['lead_time']} + {item['safety_stock']} = {rop} шт. "
                        f"Заказать: max(0, {rop} - {item['stock']}) = {item['recommended_qty']} шт."
                    )
                    forecasts.append({
                        "item_name": item['name'],
                        "sku": item['sku'],
                        "reason": reason,
                    })

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
                                base_demand = ?, status = '⏳ Наблюдение'
                            WHERE id = ?
                        """, (
                            calc_zero_date, item_data['recommended_qty'], f['reason'],
                            avg_s, item_data['lead_time'], item_data['safety_stock'],
                            item_data['reorder_point'], existing[0]
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
                             avg_daily_sales, lead_time_days, safety_stock, base_demand)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            f['item_name'], f['sku'], calc_zero_date,
                            item_data['recommended_qty'], f['reason'],
                            avg_s, item_data['lead_time'], item_data['safety_stock'],
                            item_data['reorder_point']
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

