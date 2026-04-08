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
                    "stock": row['current_qty'],
                    "avg_sales": round(avg_sales, 2)
                })

            prompt = f"""
            Ты директор по закупкам. Проанализируй список товаров и дай прогноз.
            Учти: доставка занимает 14 дней. Нужно обеспечить запас на 30 дней.
            
            Список товаров:
            {json.dumps(items_data, ensure_ascii=False)}
            
            ВЕРНИ СТРОГО JSON МАССИВ ОБЪЕКТОВ:
            [
              {{"item_name": "...", "sku": "...", "predicted_zero_date": "YYYY-MM-DD", "recommended_qty": 100, "reason": "..."}}
            ]
            """
            
            try:
                res = client.models.generate_content(model="gemini-3.1-flash-lite-preview", contents=[prompt])
                forecasts = json.loads(res.text.replace("```json", "").replace("```", "").strip())
                
                for f in forecasts:
                    avg_s = next((item['avg_sales'] for item in items_data if item['name'] == f['item_name']), 0)
                    
                    # 1. Сначала отменяем старые несыгравшие прогнозы по этому товару
                    conn.execute("""
                        UPDATE ai_forecasts 
                        SET status = '🔄 Пересчитан ИИ' 
                        WHERE item_name = ? AND status = '⏳ Наблюдение'
                    """, (f['item_name'],))
                    
                    # 2. Затем записываем новый, самый актуальный прогноз
                    conn.execute("""
                        INSERT INTO ai_forecasts (item_name, sku, predicted_zero_date, recommended_qty, reason, avg_daily_sales)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (f['item_name'], f['sku'], f['predicted_zero_date'], f['recommended_qty'], f['reason'], avg_s))
                conn.commit()
                success_count += len(forecasts)
                time.sleep(6) 
                
            except Exception as e:
                err_msg = str(e)
                logging.error(f"❌ Ошибка пакета: {err_msg}")
                return f"error_{err_msg}" # <--- ТЕПЕРЬ МЫ ВОЗВРАЩАЕМ ОШИБКУ!
                
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