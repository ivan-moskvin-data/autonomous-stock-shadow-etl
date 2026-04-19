import os
import json
import time
import logging
import sqlite3
import tomllib
import pandas as pd
import requests
from pathlib import Path
from google import genai
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
        return tomllib.load(f).get("GEMINI_API_KEY")

@st.cache_data(ttl=60, show_spinner=False)
def check_gemini_connection() -> bool:
    """Проверяет доступность серверов Google (работает ли прокси)"""
    try:
        proxies = {
            "http": "socks5://127.0.0.1:1080",
            "https": "socks5://127.0.0.1:1080"
        }
        requests.get("https://generativelanguage.googleapis.com", timeout=1.5, proxies=proxies)
        return True
    except:
        return False

def get_ai_client():
    """Инициализирует клиента с прокси"""
    api_key = get_api_key()
    if not api_key: return None
    
    os.environ['HTTPS_PROXY'] = "socks5://127.0.0.1:1080"
    os.environ['HTTP_PROXY'] = "socks5://127.0.0.1:1080"
    return genai.Client(api_key=api_key)

# ==========================================
# АГЕНТ 1: ОЦИФРОВКА НАКЛАДНЫХ (VISION)
# ==========================================
def digitize_invoice(image_file) -> list:
    """Принимает файл картинки, возвращает список словарей (JSON)"""
    client = get_ai_client()
    if not client:
        raise ValueError("API ключ не найден.")
        
    img = Image.open(image_file)
    
    prompt = """
    Ты — точный алгоритм оцифровки документов. 
    На этой картинке таблица с товарами (накладная). 
    
    ТВОЯ ЗАДАЧА:
    Извлечь данные из ячеек "Артикул", "Товары" и "Кол-во" СТРОГО 1 в 1 как напечатано на бумаге.
    
    ПРАВИЛА:
    1. Название: Перепиши весь текст ячейки полностью. Обязательно сохраняй все скобки, размеры и приписки (например, "(L=53мм) - рычаг (10/100шт.)"). Ничего не сокращай!
    2. Артикул: Перепиши всё содержимое ячейки. Если там есть название бренда (например, "Джакомини Рус") или перенос строки, склей это в одну строку и сохрани. Не обрезай текст!
    3. Количество: Верни только цифру.
    
    ВЕРНИ СТРОГО МАССИВ JSON И БОЛЬШЕ НИЧЕГО. 
    Формат:
    [
        {"название": "Кран шаровый латунный... (L=53мм) - рычаг...", "артикул": "R850X023 Джакомини Рус", "количество": 100}
    ]
    """
    
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt, img]
    )
    
    raw_text = response.text.replace("```json", "").replace("```", "").strip()
    return json.loads(raw_text)

# ==========================================
# АГЕНТ 2: ПРОГНОЗ ОСАТКОВ (FORECASTING)
# ==========================================
def run_batch_forecast():
    """Запускает массовый анализ оборачиваемости (бывший ai_forecaster.py)"""
    client = get_ai_client()
    if not client: return "no_key"

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
                df_hist = pd.read_sql_query("SELECT SUBSTR(report_timestamp, 1, 10) as date, quantity FROM stocks WHERE item_name = ? AND report_timestamp >= date('now', '-30 days')", 
                                            conn, params=(row['item_name'],))
                sales = float(df_hist['quantity'].max() - df_hist['quantity'].min())
                days_tracked = max(1, (pd.to_datetime(df_hist['date']).max() - pd.to_datetime(df_hist['date']).min()).days) if len(df_hist) > 1 else 1
                avg_sales = sales / days_tracked
                items_data.append({"name": row['item_name'], "sku": row['sku'], "stock": int(row['current_qty']), "avg_sales": round(avg_sales, 2)})

            today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
            prompt = f"""Ты — эксперт-аналитик. Сегодня: {today_date}.
            ДАННЫЕ: {json.dumps(items_data, ensure_ascii=False)}
            ПРАВИЛА:
            1. 'days_to_zero' — через СКОЛЬКО ДНЕЙ кончится товар (целое число).
            2. 'item_name' и 'sku' возвращай без изменений.
            3. Если продажи (avg_sales) = 0, 'days_to_zero': 999.
            4. 'reason' — краткое обоснование на РУССКОМ.
            ВЕРНИ СТРОГО JSON МАССИВ: [ {{"item_name": "...", "sku": "...", "days_to_zero": 10, "recommended_qty": 50, "reason": "..."}} ]"""
            
            for attempt in range(3):
                try:
                    res = client.models.generate_content(
                        model='gemini-2.5-flash',
                        contents=prompt,
                        config={"temperature": 0.1, "response_mime_type": "application/json"}
                    )
                    forecasts = json.loads(res.text.replace("```json", "").replace("```", "").strip())
                    break 
                except Exception as e:
                    if "503" in str(e) or "429" in str(e) or "UNAVAILABLE" in str(e):
                        if attempt < 2: time.sleep([5, 15, 45][attempt])
                        else: return f"error_{str(e)}"
                    else: return f"error_{str(e)}"
            
            if forecasts:
                for f in forecasts:
                    avg_s = next((item['avg_sales'] for item in items_data if item['name'] == f['item_name']), 0)
                    days = int(f.get('days_to_zero', 30))
                    calc_zero_date = (pd.Timestamp.now() + pd.Timedelta(days=days)).strftime('%Y-%m-%d')
                    
                    existing_today = conn.execute("SELECT id FROM ai_forecasts WHERE item_name = ? AND date(created_at) = date('now', 'localtime')", (f['item_name'],)).fetchone()
                    if existing_today:
                        conn.execute("UPDATE ai_forecasts SET predicted_zero_date = ?, recommended_qty = ?, reason = ?, avg_daily_sales = ?, status = '⏳ Наблюдение' WHERE id = ?", 
                                     (calc_zero_date, f['recommended_qty'], f['reason'], avg_s, existing_today[0]))
                    else:
                        conn.execute("UPDATE ai_forecasts SET status = '🔄 Пересчитан ИИ' WHERE item_name = ? AND status = '⏳ Наблюдение'", (f['item_name'],))
                        conn.execute("INSERT INTO ai_forecasts (item_name, sku, predicted_zero_date, recommended_qty, reason, avg_daily_sales) VALUES (?, ?, ?, ?, ?, ?)", 
                                     (f['item_name'], f['sku'], calc_zero_date, f['recommended_qty'], f['reason'], avg_s))
                conn.commit()
                success_count += len(forecasts)
                time.sleep(6)
                
        return f"ok_{success_count}"

if __name__ == "__main__":
    # Для тестов напрямую из консоли
    pass