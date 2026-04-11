import sqlite3
from pathlib import Path

# Вычисляем корень проекта: поднимаемся на два уровня вверх от самого скрипта
BASE_DIR = Path(__file__).resolve().parent.parent 
DB_PATH = BASE_DIR / "data" / "stock_history.sqlite"

def reset_forecasts():
    # Проверяем, существует ли база данных
    if not DB_PATH.exists():
        print(f"❌ База данных не найдена по пути:\n{DB_PATH}")
        return

    print(f"🛠 Подключаемся к базе:\n{DB_PATH}")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # 1. Стираем все старые прогнозы
        cursor.execute("DELETE FROM ai_forecasts")
        
        # 2. Сбрасываем счетчик ID
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='ai_forecasts'")
        
        conn.commit()
        
        print("✅ Журнал прогнозов ИИ успешно очищен! Можно запускать A/B тест.")

if __name__ == "__main__":
    reset_forecasts()