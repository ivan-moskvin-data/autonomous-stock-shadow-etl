import os
import json
import time
import logging
import sqlite3
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
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"
SECRETS_PATH = BASE_DIR / "src" / ".streamlit" / "secrets.toml"

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
        "model": "google/gemini-2.5-flash",
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_str}"}}
                ]
            }
        ],
        "temperature": 0.1
    }
    
    raw_text = call_openrouter(payload)
    return json.loads(raw_text.replace("```json", "").replace("```", "").strip())

# ==========================================
# АГЕНТ 2: ПРОГНОЗ ОСАТКОВ (FORECASTING)
# ==========================================
def run_batch_forecast():
    if not get_api_key(): return "no_key"

    with sqlite3.connect(DB_PATH) as conn:
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

        if active_items.empty: return "empty"

        batch_size = 10
        success_count = 0
        
        for i in range(0, len(active_items), batch_size):
            batch = active_items.iloc[i:i+batch_size]
            items_data = []
            for _, row in batch.iterrows():
                df_hist = pd.read_sql_query("SELECT SUBSTR(report_timestamp, 1, 10) as date, quantity FROM stocks WHERE item_name = ? AND report_timestamp >= date('now', '-30 days')", conn, params=(row['item_name'],))
                sales = float(df_hist['quantity'].max() - df_hist['quantity'].min())
                days_tracked = max(1, (pd.to_datetime(df_hist['date']).max() - pd.to_datetime(df_hist['date']).min()).days) if len(df_hist) > 1 else 1
                items_data.append({"name": row['item_name'], "sku": row['sku'], "stock": int(row['current_qty']), "avg_sales": round(sales / days_tracked, 2)})

            today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            prompt = f"Сегодня: {today_date}. ДАННЫЕ: {json.dumps(items_data, ensure_ascii=False)}. ПРАВИЛА: 1. 'days_to_zero' — через сколько дней кончится товар. 2. 'reason' — обоснование. ВЕРНИ JSON: [ {{\"item_name\": \"...\", \"sku\": \"...\", \"days_to_zero\": 10, \"recommended_qty\": 50, \"reason\": \"...\"}} ]"
            
            payload = {"model": "google/gemini-2.5-flash", "messages": [{"role": "user", "content": prompt}], "temperature": 0.1}
            
            for attempt in range(3):
                try:
                    raw_text = call_openrouter(payload)
                    forecasts = json.loads(raw_text.replace("```json", "").replace("```", "").strip())
                    for f in forecasts:
                        avg_s = next((item['avg_sales'] for item in items_data if item['name'] == f['item_name']), 0)
                        days = int(f.get('days_to_zero', 30))
                        calc_zero_date = (pd.Timestamp.now() + pd.Timedelta(days=days)).strftime('%Y-%m-%d')
                        existing = conn.execute("SELECT id FROM ai_forecasts WHERE item_name = ? AND date(created_at) = date('now', 'localtime')", (f['item_name'],)).fetchone()
                        if existing:
                            conn.execute("UPDATE ai_forecasts SET predicted_zero_date = ?, recommended_qty = ?, reason = ?, avg_daily_sales = ?, status = '⏳ Наблюдение' WHERE id = ?", (calc_zero_date, f['recommended_qty'], f['reason'], avg_s, existing[0]))
                        else:
                            conn.execute("UPDATE ai_forecasts SET status = '🔄 Пересчитан ИИ' WHERE item_name = ? AND status = '⏳ Наблюдение'", (f['item_name'],))
                            conn.execute("INSERT INTO ai_forecasts (item_name, sku, predicted_zero_date, recommended_qty, reason, avg_daily_sales) VALUES (?, ?, ?, ?, ?, ?)", (f['item_name'], f['sku'], calc_zero_date, f['recommended_qty'], f['reason'], avg_s))
                    conn.commit()
                    success_count += len(forecasts)
                    time.sleep(2)
                    break
                except Exception as e:
                    if attempt == 2: return f"error_{str(e)}"
                    time.sleep(2)
                    
        return f"ok_{success_count}"
