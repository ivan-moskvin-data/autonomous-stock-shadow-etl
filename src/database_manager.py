import sqlite3
import os

def create_db():
    # 1. Определяем путь к файлу. Он будет лежать в папке data под названием stock_history.sqlite
    db_path = os.path.join('data', 'stock_history.sqlite')
    
    # 2. Подключаемся. Если файла нет, Python создаст его автоматически.
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. Создаем таблицу stocks. 
    # Это наш "складской журнал", где каждая строка — это состояние одного товара на конкретную дату.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, -- Уникальный номер записи
            report_timestamp TEXT NOT NULL,       -- Дата и время, когда мы скачали данные
            sku TEXT,                             -- Артикул товара (код)
            item_name TEXT NOT NULL,              -- Название товара
            price REAL,                           -- Цена (используем REAL для чисел с запятой)
            quantity INTEGER,                     -- Остаток на складе (в штуках)
            total_value REAL,                     -- Цена * Остаток (для оценки капитала)
            category TEXT                         -- Категория (фитинги, трубы и т.д.)
            product_url TEXT                      -- Ссылка для быстрой проверки
        )
    ''')

    # 4. Сохраняем изменения и закрываем соединение
    conn.commit()
    conn.close()
    print(f"✅ База данных успешно создана и готова к работе!")
    print(f"Файл находится здесь: {db_path}")

# Этот блок кода выполнится, только если мы запустим файл напрямую
if __name__ == "__main__":
    create_db()