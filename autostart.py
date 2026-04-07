import json
import subprocess
import time
import sys
from datetime import datetime
from pathlib import Path

import requests

# --- БАЗОВЫЕ ПУТИ ---
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
LAST_RUN_FILE = BASE_DIR / "logs" / "last_run.date"
PARSER_SCRIPT = BASE_DIR / "src" / "parser.py"
FORECASTER_SCRIPT = BASE_DIR / "src" / "ai_forecaster.py"

if sys.platform == "win32":
    VENV_PYTHON = BASE_DIR / "venv" / "Scripts" / "python.exe"
else:
    VENV_PYTHON = BASE_DIR / "venv" / "bin" / "python"

# --- ДОСТАЕМ URL ИЗ КОНФИГА ---
def get_target_url() -> str:
    if CONFIG_PATH.exists():
        try:
            with CONFIG_PATH.open('r', encoding='utf-8') as f:
                return json.load(f).get('site', {}).get('base_url', "https://kaliningradbereg.ru")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"⚠️ Ошибка чтения конфига: {e}. Использую дефолтный URL.")
    return "https://kaliningradbereg.ru"

TARGET_URL = get_target_url()

# --- ЛОГИКА АВТОЗАПУСКА ---
def is_weekday() -> bool:
    # 0 = Понедельник, 4 = Пятница
    return datetime.now().astimezone().date().weekday() < 5

def already_ran_today() -> bool:
    if not LAST_RUN_FILE.exists():
        return False
    # Pathlib позволяет читать файл в одну строку
    return LAST_RUN_FILE.read_text(encoding="utf-8").strip() == str(datetime.now().astimezone().date())

def mark_as_run() -> None:
    LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Pathlib позволяет писать в файл в одну строку
    LAST_RUN_FILE.write_text(str(datetime.now().astimezone().date()), encoding="utf-8")

def wait_for_internet(timeout_mins: int = 20) -> bool:
    print(f"⏳ Ожидание подключения к {TARGET_URL}...")
    end_time = time.time() + (timeout_mins * 60)
    
    with requests.Session() as session:
        while time.time() < end_time:
            try:
                # Используем HEAD вместо GET. Нам нужны только заголовки.
                response = session.head(TARGET_URL, timeout=5)
                # 405 Method Not Allowed значит, что сервер жив, но не любит HEAD
                if response.status_code < 400 or response.status_code == 405:
                    print("✅ Интернет и целевой сайт доступны!")
                    return True
            except requests.RequestException:
                time.sleep(10)
                
    return False

def main() -> None:
    print("=" * 50)
    print("🤖 Autonomous Stock Shadow: Менеджер запуска")
    print("=" * 50)

    if not is_weekday():
        print("🛑 Сегодня выходной. Отдыхаем.")
        time.sleep(5)
        return

    if already_ran_today():
        print("🛑 Парсер уже успешно отработал сегодня. Ждем до завтра.")
        time.sleep(5)
        return

    if wait_for_internet(timeout_mins=20):
        print("🚀 Запускаем парсер...")
        time.sleep(2)
        
        try:
            # check=True выбросит ошибку, если парсер упадет с критом
            result = subprocess.run([str(VENV_PYTHON), str(PARSER_SCRIPT)], check=True)
            
            # Ставим отметку ТОЛЬКО если скрипт завершился успешно (код 0)
            if result.returncode == 0:
                mark_as_run()
                print("✅ Парсинг успешно завершен. Отметка установлена.")
                
                # --- НОВЫЙ БЛОК: Запуск ИИ после парсера ---
                print("🧠 Запускаем фоновый AI-анализ закупок (Shadow Mode)...")
                try:
                    subprocess.run([str(VENV_PYTHON), str(FORECASTER_SCRIPT)], check=True)
                    print("✅ AI-прогнозы успешно сгенерированы.")
                except subprocess.CalledProcessError as e:
                    print(f"❌ Ошибка во время работы AI-прогнозиста. Код выхода: {e.returncode}.")
                # -------------------------------------------
                
        except subprocess.CalledProcessError as e:
            print(f"❌ Ошибка во время работы парсера. Код выхода: {e.returncode}. Отметка не поставлена.")
            time.sleep(10)
    else:
        print("❌ Не дождались интернета за 20 минут. Запуск отменен.")
        time.sleep(10)

if __name__ == "__main__":
    main()