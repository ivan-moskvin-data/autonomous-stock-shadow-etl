import sys
import logging
import sqlite3
import pandas as pd
from pathlib import Path

# Обеспечиваем работу абсолютных импортов при запуске из корня проекта
sys.path.insert(0, str(Path(__file__).resolve().parent))
from queries import get_anomalies_query

# --- НАСТРОЙКИ ---
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"

def analyze_changes() -> None:
    """
    Анализирует аномальные поступления товаров (Silent Cancellation) средствами SQL.
    Оптимизировано: строгая типизация, именованные параметры и быстрый срез дат (SUBSTR).
    """
    if not DB_PATH.exists():
        logging.error("❌ База данных не найдена. Сначала запустите парсер!")
        return

    with sqlite3.connect(DB_PATH) as conn:
        # 1. Быстрое получение уникальных дат "родным" курсором (без оверхеда Pandas)
        # SUBSTR работает быстрее DATE(), так как просто режет строку $O(1)$ без календарной логики
        cursor = conn.execute("SELECT DISTINCT SUBSTR(report_timestamp, 1, 10) FROM stocks ORDER BY 1 DESC LIMIT 2")
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) < 2:
            logging.warning("⚠️ Недостаточно данных для анализа. Нужно хотя бы два дня записей.")
            return

        today_str, yesterday_str = dates[0], dates[1]
        logging.info(f"📊 Анализируем изменения: {yesterday_str} -> {today_str}")

        # 2. SQL-запрос с именованными параметрами (:today, :yesterday). Оптимизированный SQL-запрос импортируется из src/queries.py
        query = get_anomalies_query() + " ORDER BY delta DESC"

        
        # Передаем параметры через словарь — это защищает от путаницы в порядке аргументов
        params = {"today": today_str, "yesterday": yesterday_str}
        
        anomalies = pd.read_sql_query(query, conn, params=params)

        # 3. Вывод результата
        if not anomalies.empty:
            logging.info(f"\n🚀 ОБНАРУЖЕНО АНОМАЛИЙ: {len(anomalies)}")
            print(anomalies.to_string(index=False))
        else:
            logging.info("\n✅ Резких скачков остатков (положительных дельт) не обнаружено.")

if __name__ == "__main__":
    analyze_changes()