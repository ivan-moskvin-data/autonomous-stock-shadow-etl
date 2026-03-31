import json
import logging
import re
import sqlite3
import time
import random
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn
from rich.console import Group

# --- ЗАГРУЗКА КОНФИГУРАЦИИ ---
DEFAULT_CONFIG = {
    "site": {
        "base_url": "https://kaliningradbereg.ru",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    },
    "paths": {
        "data_dir": "data",
        "log_dir": "logs",
        "db_name": "stock_history.sqlite"
    },
    "crawler": {
        "timeout": 10,
        "max_errors": 10,
        "sleep_min": 1.2,
        "sleep_max": 3.5
    }
}

def load_config() -> dict:
    # Находим путь к конфигу в корне проекта
    config_path = Path(__file__).resolve().parent.parent / "config.json"
    
    if not config_path.exists():
        # Страховка: если файла нет, не падаем, а создаем дефолтный
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
        print(f"⚙️ Создан базовый файл конфигурации: {config_path}")
        return DEFAULT_CONFIG
        
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)

CONFIG = load_config()

# --- ПРИМЕНЕНИЕ НАСТРОЕК ---
BASE_URL = CONFIG['site']['base_url']
HEADERS = CONFIG['site']['headers']
START_PATH = CONFIG['site'].get('start_path', '/katalog/') # Добавлено получение start_path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / CONFIG['paths']['data_dir']
LOG_DIR = BASE_DIR / CONFIG['paths']['log_dir']

DB_PATH = DB_DIR / CONFIG['paths']['db_name']
STATE_PATH = DB_DIR / "crawler_state.json"
LOG_FILE = LOG_DIR / "parser.log"

TIMEOUT = CONFIG['crawler']['timeout']
MAX_ERRORS = CONFIG['crawler']['max_errors']
SLEEP_RANGE = (CONFIG['crawler']['sleep_min'], CONFIG['crawler']['sleep_max'])

# --- НАСТРОЙКА ЛОГГИРОВАНИЯ ---
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)

# --- МОДЕЛЬ ДАННЫХ ---
@dataclass(slots=True)
class Product:
    timestamp: str
    name: str
    sku: Optional[str]
    price: float
    stock: int
    category: str
    url: str

    @property
    def total_value(self) -> float:
        return self.price * self.stock

def init_db() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    
    if DB_PATH.exists():
        logging.info(f"🐚 Подключаюсь к существующей базе: {DB_PATH}")
    else:
        logging.warning(f"🆕 База не найдена. Создаю новую по адресу: {DB_PATH}")
        
    conn = sqlite3.connect(DB_PATH)
    
    with conn:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_timestamp TEXT NOT NULL,
                sku TEXT,
                item_name TEXT NOT NULL,
                price REAL,
                quantity INTEGER,
                total_value REAL,
                category TEXT,
                product_url TEXT
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_report_date ON stocks(report_timestamp);')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_daily_item ON stocks(SUBSTR(report_timestamp, 1, 10), item_name);')
        
        # 1. СТАРАЯ ТАБЛИЦА: ОСНОВНАЯ ИСТОРИЯ СКЛАДА (остается для совместимости)
        # 2. НОВАЯ ТАБЛИЦА: РЕЕСТР ОПЕРАЦИОННЫХ АНОМАЛИЙ
        conn.execute('''
            CREATE TABLE IF NOT EXISTS anomaly_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at TEXT NOT NULL,          -- Дата фиксации
                item_name TEXT NOT NULL,            -- Название товара
                anomaly_type TEXT NOT NULL,         -- Тип (Пересорт, Ошибка 1С и т.д.)
                qty_system INTEGER,                 -- Остаток в базе (на сайте)
                qty_physical INTEGER,               -- Твой реальный пересчет
                financial_impact REAL,              -- Цена ошибки (разница * цена)
                source TEXT NOT NULL,               -- 'Автоматически' или 'Вручную'
                status TEXT NOT NULL,               -- 'Открыта' или 'Закрыта'
                resolved_at TEXT,                   -- Дата, когда вопрос решился
                comment TEXT                        -- Твои заметки
            )
        ''')
        # Индекс для мгновенного поиска всех проблем по конкретному товару
        conn.execute('CREATE INDEX IF NOT EXISTS idx_anomaly_item ON anomaly_log(item_name);')
        
    return conn

def save_to_db(conn: sqlite3.Connection, products: List[Product]) -> None:
    if not products: return
    data_to_insert = [
        (p.timestamp, p.sku, p.name, p.price, p.stock, p.total_value, p.category, p.url)
        for p in products
    ]
    try:
        conn.executemany('''
            INSERT INTO stocks (report_timestamp, sku, item_name, price, quantity, total_value, category, product_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(SUBSTR(report_timestamp, 1, 10), item_name) DO UPDATE SET
                report_timestamp = excluded.report_timestamp,
                sku = excluded.sku,
                price = excluded.price,
                quantity = excluded.quantity,
                total_value = excluded.total_value,
                category = excluded.category,
                product_url = excluded.product_url
        ''', data_to_insert)
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"❌ Ошибка БД: {e}")

def load_state() -> Optional[Tuple[deque, Set[str], int]]:
    """O(1) загрузка чекпоинта."""
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logging.info("♻️ Восстанавливаем прерванный сеанс из чекпоинта...")
            return deque(data['queue']), set(data['seen_urls']), data['total_scraped']
        except Exception as e:
            logging.error(f"⚠️ Ошибка чтения чекпоинта: {e}. Начинаем заново.")
    return None

def save_state(queue: deque, seen_urls: Set[str], total_scraped: int) -> None:
    """O(N) сериализация состояния."""
    state = {
        'queue': list(queue),
        'seen_urls': list(seen_urls),
        'total_scraped': total_scraped
    }
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(state, f)

def clear_state() -> None:
    STATE_PATH.unlink(missing_ok=True)
    logging.info("🧹 Чекпоинт удален.")

# --- СЕТЬ ---
def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

# --- ИЗВЛЕЧЕНИЕ ТОВАРОВ ИЗ HTML ---
def extract_products(soup: BeautifulSoup, url: str) -> List[Product]:
    table = soup.find('table', class_='goods')
    if not table: return []
    
    rows = table.find_all('tr', recursive=False)
    if len(rows) <= 1: return []

    breadcrumb = soup.find('ul', class_='breadcrumb')
    category_name = breadcrumb.find('li', class_='active').get_text(strip=True) if breadcrumb else "Общая категория"

    products = []
    current_time = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')

    for row in rows[1:]:
        cols = row.find_all('td', recursive=False)
        if len(cols) < 4: continue
        
        name = cols[1].get_text(strip=True)
        sku = cols[2].get_text(strip=True) or None
        
        price_span = cols[3].find('span', class_='actual')
        price = 0.0
        if price_span:
            try:
                price = float(price_span.get_text(strip=True).replace(" ", ""))
            except ValueError: pass

        stock_div = cols[3].find('div', class_='warehouse')
        qty = 0
        if stock_div:
            match = re.search(r'\d+', stock_div.get_text())
            qty = int(match.group()) if match else 0

        link_tag = cols[1].find('a')
        product_url = urljoin(BASE_URL, link_tag.get('href', '')) if link_tag else url
        products.append(Product(current_time, name, sku, price, qty, category_name, product_url))
        
    return products

# --- УМНЫЙ КРАУЛЕР С ЧЕКПОИНТАМИ И ИНТЕРФЕЙСОМ RICH ---
def run_smart_crawler(session: requests.Session, db_conn: sqlite3.Connection) -> int:
    state = load_state()
    if state:
        queue, seen_urls, total_scraped = state
    else:
        start_url = urljoin(BASE_URL, START_PATH)
        queue = deque([start_url])
        seen_urls = {start_url}
        total_scraped = 0

    consecutive_errors = 0
    stats = {"ok": 0, "error": 0}

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[cyan]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        "• Осталось:", TimeRemainingColumn(),
        "• Прошло:", TimeElapsedColumn(),
    )
    task_id = progress.add_task("Инициализация...", total=1)

    def generate_dashboard(current_url: str):
        pages_in_q = sum(1 for u in queue if "?p=" in u)
        folders_in_q = len(queue) - pages_in_q

        table = Table(show_header=False, expand=True, box=None)
        table.add_row("💎 Собрано товаров:", f"[bold yellow]{total_scraped}[/]")
        table.add_row("📂 Папок в очереди:", f"[bold cyan]{folders_in_q}[/]")
        table.add_row("📄 Страниц пагинации:", f"[bold blue]{pages_in_q}[/]")
        table.add_row("---", "---")
        table.add_row("✅ Успешных страниц (HTTP 200):", f"[green]{stats['ok']}[/]")
        table.add_row("⚠️  Ошибок сети:", f"[red]{stats['error']}[/]")

        group = Group(table, "", progress)
        
        return Panel(
            group, 
            title="[bold magenta]Autonomous Stock Shadow[/]", 
            subtitle=f"Текущая цель: {current_url[:60]}... | Сбоев подряд: {consecutive_errors}/{MAX_ERRORS}",
            border_style="bright_blue"
        )

    with Live(generate_dashboard("Старт"), refresh_per_second=4) as live:
        
        while queue:
            if consecutive_errors >= MAX_ERRORS:
                logging.critical(f"⛔ КРИТИЧЕСКАЯ ОШИБКА: {MAX_ERRORS} сбоев подряд. Парсинг прерван.")
                break

            current_url = queue.popleft()
            logging.info(f"🔍 Сканирую: {current_url}")
            
            total_tasks = stats['ok'] + stats['error'] + len(queue) + 1
            progress.update(task_id, total=total_tasks, description="Сбор данных...")

            try:
                response = session.get(current_url, timeout=TIMEOUT)
                response.raise_for_status()
                
                consecutive_errors = 0
                stats["ok"] += 1
                progress.update(task_id, advance=1)
                
                soup = BeautifulSoup(response.text, 'lxml')

                products = extract_products(soup, current_url)
                if products:
                    save_to_db(db_conn, products)
                    total_scraped += len(products)
                    
                    pages_block = soup.find('div', class_='pages')
                    if pages_block:
                        for page_link in pages_block.find_all('a'):
                            href = page_link.get('href')
                            if href and '?p=' in href:
                                full_page_url = urljoin(current_url, href)
                                if full_page_url not in seen_urls:
                                    seen_urls.add(full_page_url)
                                    queue.append(full_page_url)
                else:
                    for folder_list in soup.find_all('ul', class_=['categories', 'catalog']):
                        for item in folder_list.find_all('li', class_='item'):
                            link_tag = item.find('a', class_='link')
                            if link_tag:
                                full_url = urljoin(BASE_URL, link_tag.get('href', ''))
                                if full_url not in seen_urls:
                                    seen_urls.add(full_url)
                                    queue.append(full_url)
                
                save_state(queue, seen_urls, total_scraped)
                live.update(generate_dashboard(current_url))
                time.sleep(random.uniform(*SLEEP_RANGE))

            except requests.RequestException as e:
                consecutive_errors += 1
                stats["error"] += 1
                logging.error(f"❌ Ошибка сети: {e}")
                queue.appendleft(current_url) 
                
                progress.update(task_id, description="[red]Ожидание после ошибки...[/]")
                live.update(generate_dashboard(current_url))
                time.sleep(5)

    if not queue and consecutive_errors < MAX_ERRORS:
        clear_state()
        
    return total_scraped

def main() -> None:
    start_time = time.time()
    db_conn = init_db()
    session = get_session()
    
    try:
        run_smart_crawler(session, db_conn)
    finally:
        session.close()
        db_conn.close()
        
    elapsed_seconds = time.time() - start_time
    logging.info(f"⏱️ Общее время: {int(elapsed_seconds // 60)} мин {int(elapsed_seconds % 60)} сек")

if __name__ == "__main__":
    main()