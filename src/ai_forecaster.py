import sqlite3
import pandas as pd
import json
import time
import logging
import tomllib
from pathlib import Path
from google import genai

# --- НАСТРОЙКИ ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"
SECRETS_PATH = BASE_DIR / "src" / ".streamlit" / "secrets.toml"

def get_api_key():
    if not SECRETS_PATH.exists(): return None
    with open(SECRETS_PATH, "rb") as f:
        return tomllib.load(f).get("GEMINI_API_KEY")

def run_batch_forecast():
    api_key = get_api_key()
    if not api_key: 
        return "no_key"

    import os
    os.environ['HTTPS_PROXY'] = "socks5://127.0.0.1:1080"
    os.environ['HTTP_PROXY'] = "socks5://127.0.0.1:1080"
    logging.info("🛡️ Прокси активирован для сессии ИИ")

    client = genai.Client(api_key=api_key)

    with sqlite3.connect(DB_PATH) as conn:
        # Находим товары, которые реально продавались (падал остаток)
        active_items = pd.read_sql_query("""
            SELECT 
                item_name, sku,
                MAX(quantity) as peak_qty,
                (SELECT quantity FROM stocks s2 WHERE s2.item_name = s.item_name ORDER BY report_timestamp DESC LIMIT 1) as current_qty,
                (SELECT price FROM stocks s3 WHERE s3.item_name = s.item_name ORDER BY report_timestamp DESC LIMIT 1) as price
            FROM stocks s
            WHERE report_timestamp >= date('now', '-30 days', 'localtime')
            GROUP BY item_name
            HAVING current_qty < peak_qty AND current_qty > 0
            ORDER BY (peak_qty - current_qty) DESC
            LIMIT 30
        """, conn)

        if active_items.empty:
            return "empty" # Возвращаем статус, что прогнозировать нечего

        batch_size = 10
        success_count = 0
        
        for i in range(0, len(active_items), batch_size):
            batch = active_items.iloc[i:i+batch_size]
            
            items_data = []
            for _, row in batch.iterrows():
                # УМНЫЙ РАСЧЕТ ДНЕЙ (а не жестко 30)
                df_hist = pd.read_sql_query("SELECT SUBSTR(report_timestamp, 1, 10) as date, quantity FROM stocks WHERE item_name = ? AND report_timestamp >= date('now', '-30 days')", 
                                            conn, params=(row['item_name'],))
                
                sales = float(df_hist['quantity'].max() - df_hist['quantity'].min())
                
                # Если истории мало, считаем по фактическим дням в базе
                if len(df_hist) > 1:
                    df_hist['date'] = pd.to_datetime(df_hist['date'])
                    days_tracked = max(1, (df_hist['date'].max() - df_hist['date'].min()).days)
                else:
                    days_tracked = 1
                    
                avg_sales = sales / days_tracked
                
                items_data.append({
                    "name": row['item_name'],
                    "sku": row['sku'],
                    "stock": int(row['current_qty']),
                    "avg_sales": round(avg_sales, 2)
                })

            # Добавляем текущую дату, чтобы ИИ не бредил датами из прошлого
            today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            
            prompt = f"""
            Ты — эксперт-аналитик по закупкам. Сегодня: {today_date}.
            
            ТВОЯ ЗАДАЧА:
            Рассчитать через сколько дней обнулится остаток и какой нужен объем заказа.
            
            ДАННЫЕ:
            {json.dumps(items_data, ensure_ascii=False)}
            
            ПРАВИЛА (СТРОГО):
            1. 'days_to_zero' — это через СКОЛЬКО ДНЕЙ кончится товар (остаток / продажи_в_день). Это должно быть целое число!
            2. 'item_name' и 'sku' возвращай СТРОГО без изменений.
            3. Если продажи (avg_sales) = 0, пиши 'days_to_zero': 999 (бесконечный запас).
            4. В 'reason' напиши краткое и понятное обоснование на РУССКОМ языке. Например: "Остаток 278 шт, при продажах 7.6 шт/день хватит на 36 дней. Запас достаточен" или "Хватит на 5 дней, нужно дозаказать 100 шт для покрытия месяца". Не используй переменные и код!
            
            ВЕРНИ СТРОГО JSON МАССИВ:
            [
              {{"item_name": "...", "sku": "...", "days_to_zero": число, "recommended_qty": число, "reason": "..."}}
            ]
            """
            
            # --- ВСТРОЕННАЯ ЗАЩИТА ОТ ОШИБКИ 503 (Экспоненциальная задержка) ---
            MAX_RETRIES = 3
            RETRY_DELAYS = [5, 15, 45]
            forecasts = None
            
            for attempt in range(MAX_RETRIES):
                try:
                    res = client.models.generate_content(
                        model='gemini-2.5-flash',
                        contents=prompt,
                        config={"temperature": 0.1, "response_mime_type": "application/json"} # Убиваем креатив
                    )
                    forecasts = json.loads(res.text.replace("```json", "").replace("```", "").strip())
                    break  # Успешно получили данные! Выходим из цикла повторов
                
                except Exception as e:
                    err_msg = str(e)
                    # Если Google перегружен (503) — ждем и пробуем снова
                    if "503" in err_msg or "429" in err_msg or "UNAVAILABLE" in err_msg:
                        if attempt < MAX_RETRIES - 1:
                            wait_time = RETRY_DELAYS[attempt]
                            logging.warning(f"⚠️ Google API перегружен. Ждем {wait_time} сек... (Попытка {attempt + 2}/{MAX_RETRIES})")
                            time.sleep(wait_time)
                        else:
                            logging.error(f"❌ Сервер не ответил после {MAX_RETRIES} попыток.")
                            return f"error_{err_msg}"
                    # Если ошибка другая (например, ИИ вернул не JSON) — падаем сразу
                    else:
                        logging.error(f"❌ Ошибка пакета: {err_msg}")
                        return f"error_{err_msg}"
            
            # Если данные успешно получены, записываем их в базу
            # Если данные успешно получены, записываем их в базу
            if forecasts:
                for f in forecasts:
                    avg_s = next((item['avg_sales'] for item in items_data if item['name'] == f['item_name']), 0)
                    days = int(f.get('days_to_zero', 30))
                    calc_zero_date = (pd.Timestamp.now() + pd.Timedelta(days=days)).strftime('%Y-%m-%d')
                    
                    existing_today = conn.execute("""
                        SELECT id FROM ai_forecasts 
                        WHERE item_name = ? AND date(created_at) = date('now', 'localtime')
                    """, (f['item_name'],)).fetchone()
                    
                    if existing_today:
                        conn.execute("""
                            UPDATE ai_forecasts 
                            SET predicted_zero_date = ?, 
                                recommended_qty = ?, 
                                reason = ?, 
                                avg_daily_sales = ?,
                                status = '⏳ Наблюдение'
                            WHERE id = ?
                        """, (calc_zero_date, f['recommended_qty'], f['reason'], avg_s, existing_today[0]))
                    else:
                        conn.execute("""
                            UPDATE ai_forecasts 
                            SET status = '🔄 Пересчитан ИИ' 
                            WHERE item_name = ? AND status = '⏳ Наблюдение'
                        """, (f['item_name'],))
                        
                        conn.execute("""
                            INSERT INTO ai_forecasts (item_name, sku, predicted_zero_date, recommended_qty, reason, avg_daily_sales)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (f['item_name'], f['sku'], calc_zero_date, f['recommended_qty'], f['reason'], avg_s))
                
                conn.commit()
                success_count += len(forecasts)
                time.sleep(6)
                
        return f"ok_{success_count}"

if __name__ == "__main__":
    status = run_batch_forecast()
    logging.info(f"Статус выполнения ИИ: {status}")
    
    # Если ИИ отработал успешно или прогнозировать пока нечего - списываем долг
    if status == "empty" or (isinstance(status, str) and status.startswith("ok_")):
        flag_path = BASE_DIR / "logs" / "ai_pending.flag"
        if flag_path.exists():
            flag_path.unlink()
            logging.info("✅ Флаг ожидания ИИ успешно снят.")