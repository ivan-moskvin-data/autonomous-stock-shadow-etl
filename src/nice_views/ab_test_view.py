"""
ab_test_view.py вЂ” NiceGUI-РІРµСЂСЃРёСЏ РІРєР»Р°РґРєРё В«A/B РўРµСЃС‚: AI vs Р§РµР»РѕРІРµРєВ».
РџРѕР»РЅС‹Р№ РїРµСЂРµРЅРѕСЃ С„СѓРЅРєС†РёРѕРЅР°Р»Р° РёР· src/views/ab_test_view.py.

verify_shadow_forecasts() РёРЅР»Р°Р№РЅРѕРІР°РЅР° РЅР°РїСЂСЏРјСѓСЋ (circular import app.py РЅРµРІРѕР·РјРѕР¶РµРЅ).
ai_services.run_batch_forecast() РІС‹Р·С‹РІР°РµС‚СЃСЏ С‡РµСЂРµР· run.io_bound().
"""
from nicegui import ui, run as ng_run
import sys
import os
import logging
import pandas as pd
from pathlib import Path

_src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

import db
import ai_services
from nice_views.shared_layout import build_shell

logger = logging.getLogger('shadow_stock.abtest')

_AI_PENDING_FLAG = Path(_src_dir).parent / 'logs' / 'ai_pending.flag'


# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
#  РРЅР»Р°Р№РЅ-РєРѕРїРёСЏ verify_shadow_forecasts (Р±РµР· app.py вЂ” circular import)
# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def _verify_shadow_forecasts() -> None:
    """
    РћР±РЅРѕРІР»СЏРµС‚ СЃС‚Р°С‚СѓСЃС‹ Р°РєС‚РёРІРЅС‹С… РїСЂРѕРіРЅРѕР·РѕРІ РїРѕ С‚РµРєСѓС‰РёРј РѕСЃС‚Р°С‚РєР°Рј.
    Р›РѕРіРёРєР° РёР· app.verify_shadow_forecasts(), Р±РµР· Р·Р°РІРёСЃРёРјРѕСЃС‚Рё РѕС‚ Streamlit/app.py.
    """
    try:
        config = db.CONFIG
        with db.get_connection() as conn:
            forecasts = pd.read_sql_query("""
                SELECT * FROM ai_forecasts
                WHERE status NOT IN (
                    'рџ“‰ РЈРїСѓС‰РµРЅРЅР°СЏ РІС‹РіРѕРґР°', 'вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·', 'рџ”„ РџРµСЂРµСЃС‡РёС‚Р°РЅ РР'
                )
            """, conn)

            if forecasts.empty:
                return

            latest_inv = db.load_inventory()
            if latest_inv.empty:
                return

            today = pd.Timestamp.now().normalize()

            for _, row in forecasts.iterrows():
                item_name = row['item_name']
                sku       = row['sku']
                db_id     = row['id']

                match = pd.DataFrame()
                if pd.notna(sku) and str(sku).strip():
                    match = latest_inv[latest_inv['РђСЂС‚РёРєСѓР»'] == sku]
                if match.empty:
                    match = latest_inv[latest_inv['РќР°РёРјРµРЅРѕРІР°РЅРёРµ'] == item_name]
                if match.empty:
                    continue

                curr_qty  = float(match.iloc[0]['РћСЃС‚Р°С‚РѕРє'])
                price     = float(match.iloc[0]['Р¦РµРЅР°'])
                avg_sales = float(row['avg_daily_sales'])

                # РџРµСЂРµСЃС‡С‘С‚ РµСЃР»Рё РёР·РјРµРЅРёР»СЃСЏ lead_time РІ РєРѕРЅС„РёРіРµ
                current_lead = config['ai']['lead_time_days']
                forecast_lead = int(row['lead_time_days']) if row['lead_time_days'] else 14
                if forecast_lead != current_lead:
                    base_demand = int(curr_qty + avg_sales * current_lead)
                    safety      = int(avg_sales * 0.2)
                    rec_qty     = base_demand + safety
                    days_to_z   = round(curr_qty / avg_sales, 1) if avg_sales > 0 else 999.0
                    zero_date   = (today + pd.Timedelta(days=int(days_to_z))).strftime('%Y-%m-%d')
                    conn.execute("""
                        UPDATE ai_forecasts
                        SET predicted_zero_date=?, recommended_qty=?,
                            lead_time_days=?, safety_stock=?, base_demand=?
                        WHERE id=?
                    """, (zero_date, rec_qty, current_lead, safety, base_demand - safety, db_id))
                    continue

                pred_date = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
                if pd.isna(pred_date):
                    pred_date = today + pd.Timedelta(days=30)

                if curr_qty <= 0:
                    effective = min(today, pred_date)
                    days_lost = max(1, (today - effective).days)
                    lost_val  = days_lost * avg_sales * price
                    conn.execute("""
                        UPDATE ai_forecasts
                        SET status='рџ”ґ РўРѕРІР°СЂ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚',
                            lost_sales_value=?, overstock_value=0
                        WHERE id=?
                    """, (lost_val, db_id))
                    continue

                if curr_qty > (avg_sales * 60):
                    overstock_qty = curr_qty - (avg_sales * 44)
                    overstock_val = max(0, overstock_qty * price)
                    conn.execute("""
                        UPDATE ai_forecasts
                        SET status='рџ§Љ РџРµСЂРµР·Р°С‚Р°СЂРєР°',
                            overstock_value=?, lost_sales_value=0
                        WHERE id=?
                    """, (overstock_val, db_id))
                else:
                    conn.execute(
                        "UPDATE ai_forecasts SET status='вЏі РќР°Р±Р»СЋРґРµРЅРёРµ' WHERE id=?",
                        (db_id,)
                    )

            conn.commit()

    except Exception:
        logger.exception('_verify_shadow_forecasts error')


# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
#  Р—Р°РіСЂСѓР·РєР° РґР°РЅРЅС‹С…
# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def _days_in_db() -> int:
    try:
        with db.get_connection() as conn:
            return conn.execute(
                "SELECT COUNT(DISTINCT SUBSTR(report_timestamp,1,10)) FROM stocks"
            ).fetchone()[0] or 0
    except Exception:
        return 0


def _forecasts_today() -> int:
    try:
        with db.get_connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM ai_forecasts "
                "WHERE date(created_at) = date('now','localtime')"
            ).fetchone()[0] or 0
    except Exception:
        return 0


def _load_forecasts() -> pd.DataFrame:
    try:
        with db.get_connection() as conn:
            df = pd.read_sql_query("""
                SELECT
                    f.*,
                    (SELECT quantity FROM stocks s
                     WHERE s.item_name = f.item_name
                     ORDER BY report_timestamp DESC LIMIT 1) AS current_qty
                FROM ai_forecasts f
                ORDER BY f.created_at DESC
            """, conn)

            if df.empty:
                return df

            # Р‘Р°С‚С‡-Р·Р°РіСЂСѓР·РєР° РёСЃС‚РѕСЂРёРё РѕСЃС‚Р°С‚РєРѕРІ РґР»СЏ РІСЃРµС… С‚РѕРІР°СЂРѕРІ РѕРґРЅРёРј Р·Р°РїСЂРѕСЃРѕРј
            # (РѕРґРёРЅ SELECT РІРјРµСЃС‚Рѕ N РѕС‚РґРµР»СЊРЅС‹С… вЂ” РјРёРЅРёРјР°Р»СЊРЅР°СЏ РЅР°РіСЂСѓР·РєР°)
            item_list = df['item_name'].dropna().unique().tolist()
            placeholders = ','.join('?' * len(item_list))
            hist_df = pd.read_sql_query(f"""
                SELECT
                    item_name,
                    SUBSTR(report_timestamp, 1, 10) AS date,
                    quantity
                FROM stocks
                WHERE item_name IN ({placeholders})
                  AND report_timestamp >= date('now', '-30 days', 'localtime')
                GROUP BY item_name, SUBSTR(report_timestamp, 1, 10)
                HAVING report_timestamp = MAX(report_timestamp)
                ORDER BY item_name, date ASC
            """, conn, params=item_list)

            # Р“СЂСѓРїРїРёСЂСѓРµРј РёСЃС‚РѕСЂРёСЋ РїРѕ С‚РѕРІР°СЂСѓ в†’ СЃРїРёСЃРѕРє [qty, qty, ...]
            history_map: dict = {}
            for name, grp in hist_df.groupby('item_name'):
                history_map[name] = grp['quantity'].tolist()

            df['sparkline'] = df['item_name'].map(
                lambda n: history_map.get(n, [])
            )
            return df

    except Exception:
        return pd.DataFrame()


# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
#  Р’СЃРїРѕРјРѕРіР°С‚РµР»СЊРЅС‹Рµ UI
# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def _status_color(status: str) -> str:
    if 'рџ“‰' in status or 'рџ”ґ' in status:
        return '#ef4444'
    if 'рџ§Љ' in status:
        return '#38bdf8'
    if 'вњ…' in status:
        return '#22c55e'
    if 'вЏі' in status or 'рџ”„' in status:
        return '#f59e0b'
    return '#9ca3af'


def _fmt_rub(val) -> str:
    try:
        v = float(val)
        return f"{v:,.0f} в‚Ѕ".replace(',', '\u202f') if v > 0 else ''
    except Exception:
        return ''


# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
#  РћС†РµРЅРєР° С‚РѕС‡РЅРѕСЃС‚Рё РїСЂРѕРіРЅРѕР·РѕРІ
# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def _check_forecast_accuracy() -> None:
    """
    РџСЂРѕРІРµСЂСЏРµС‚ С‚РѕС‡РЅРѕСЃС‚СЊ РїСЂРѕРіРЅРѕР·РѕРІ, Сѓ РєРѕС‚РѕСЂС‹С… РїСЂРѕС€Р»Рѕ РґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РІСЂРµРјРµРЅРё.

    РђР»РіРѕСЂРёС‚Рј:
    1. Р‘РµСЂС‘Рј РїСЂРѕРіРЅРѕР·С‹ РІ СЃС‚Р°С‚СѓСЃРµ 'вЏі РќР°Р±Р»СЋРґРµРЅРёРµ' РёР»Рё 'рџ”ґ РўРѕРІР°СЂ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚',
       СЃРѕР·РґР°РЅРЅС‹Рµ Р±РѕР»РµРµ lead_time_days РґРЅРµР№ РЅР°Р·Р°Рґ (РїСЂРѕС€Р»Рѕ РґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РІСЂРµРјРµРЅРё).
    2. РЎРјРѕС‚СЂРёРј С‡С‚Рѕ СЂРµР°Р»СЊРЅРѕ РїСЂРѕРёР·РѕС€Р»Рѕ СЃ РѕСЃС‚Р°С‚РєРѕРј С‚РѕРІР°СЂР° РїРѕСЃР»Рµ РґР°С‚С‹ РїСЂРѕРіРЅРѕР·Р°:
       - Р•СЃР»Рё РѕСЃС‚Р°С‚РѕРє РґРѕСЃС‚РёРі 0 РІ РїСЂРµРґРµР»Р°С… В±TOLERANCE_DAYS РѕС‚ predicted_zero_date
         в†’ 'вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·'
       - Р•СЃР»Рё РїСЂРѕРіРЅРѕР· РїСЂРµРґСЃРєР°Р·С‹РІР°Р» РѕР±РЅСѓР»РµРЅРёРµ, РЅРѕ С‚РѕРІР°СЂ РІСЃС‘ РµС‰С‘ РµСЃС‚СЊ Рё РґР°С‚Р°
         СѓР¶Рµ РїСЂРѕС€Р»Р° в†’ 'рџ“‰ РЈРїСѓС‰РµРЅРЅР°СЏ РІС‹РіРѕРґР°' (РїСЂРѕРіРЅРѕР· Р±С‹Р» РІРµСЂРµРЅ, РЅРѕ РЅРµ РєСѓРїРёР»Рё РІРѕРІСЂРµРјСЏ)
       - РРЅР°С‡Рµ РѕСЃС‚Р°РІР»СЏРµРј С‚РµРєСѓС‰РёР№ СЃС‚Р°С‚СѓСЃ (РЅР°Р±Р»СЋРґР°РµРј РґР°Р»СЊС€Рµ)
    """
    TOLERANCE_DAYS = 3  # В±3 РґРЅСЏ вЂ” В«С‚РѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·В»

    try:
        config = db.CONFIG
        lead_time = config['ai']['lead_time_days']

        with db.get_connection() as conn:
            forecasts = pd.read_sql_query(f"""
                SELECT id, item_name, sku, predicted_zero_date, avg_daily_sales,
                       created_at, status
                FROM ai_forecasts
                WHERE status IN ('вЏі РќР°Р±Р»СЋРґРµРЅРёРµ', 'рџ”ґ РўРѕРІР°СЂ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚')
                  AND date(created_at, '+{lead_time} days') <= date('now', 'localtime')
                  AND predicted_zero_date IS NOT NULL
            """, conn)

            if forecasts.empty:
                return

            for _, row in forecasts.iterrows():
                item_name   = row['item_name']
                pred_date   = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
                created_at  = pd.to_datetime(row['created_at'], errors='coerce')
                db_id       = row['id']
                avg_sales   = float(row['avg_daily_sales'] or 0)

                if pd.isna(pred_date) or pd.isna(created_at):
                    continue

                # Р‘РµСЂС‘Рј РёСЃС‚РѕСЂРёСЋ РѕСЃС‚Р°С‚РєРѕРІ РїРѕСЃР»Рµ РґР°С‚С‹ РїСЂРѕРіРЅРѕР·Р°
                window_start = created_at.strftime('%Y-%m-%d')
                window_end   = (pred_date + pd.Timedelta(days=TOLERANCE_DAYS + 5)).strftime('%Y-%m-%d')

                hist = pd.read_sql_query("""
                    SELECT SUBSTR(report_timestamp, 1, 10) AS date, quantity
                    FROM stocks
                    WHERE item_name = ?
                      AND SUBSTR(report_timestamp, 1, 10) BETWEEN ? AND ?
                    GROUP BY SUBSTR(report_timestamp, 1, 10)
                    HAVING report_timestamp = MAX(report_timestamp)
                    ORDER BY date ASC
                """, conn, params=(item_name, window_start, window_end))

                if hist.empty:
                    continue

                # РС‰РµРј РїРµСЂРІС‹Р№ РґРµРЅСЊ РєРѕРіРґР° РѕСЃС‚Р°С‚РѕРє СѓРїР°Р» РґРѕ 0 РёР»Рё РѕС‡РµРЅСЊ РЅРёР·РєРѕ (< avg/2)
                threshold = max(1, avg_sales * 0.5) if avg_sales > 0 else 1
                zero_rows = hist[hist['quantity'] <= threshold]

                if not zero_rows.empty:
                    actual_zero_date = pd.to_datetime(zero_rows.iloc[0]['date'])
                    diff_days = abs((actual_zero_date - pred_date).days)

                    if diff_days <= TOLERANCE_DAYS:
                        # РџСЂРѕРіРЅРѕР· С‚РѕС‡РЅС‹Р№!
                        conn.execute(
                            "UPDATE ai_forecasts SET status='вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·' WHERE id=?",
                            (db_id,)
                        )
                    # Р•СЃР»Рё diff > TOLERANCE вЂ” РїСЂРѕРіРЅРѕР· РѕС€РёР±СЃСЏ, РѕСЃС‚Р°РІР»СЏРµРј С‚РµРєСѓС‰РёР№ СЃС‚Р°С‚СѓСЃ

            conn.commit()

    except Exception:
        logger.exception('_check_forecast_accuracy error')


def _load_accuracy_stats() -> dict:
    """
    Р’РѕР·РІСЂР°С‰Р°РµС‚ Р°РіСЂРµРіРёСЂРѕРІР°РЅРЅСѓСЋ СЃС‚Р°С‚РёСЃС‚РёРєСѓ С‚РѕС‡РЅРѕСЃС‚Рё РїСЂРѕРіРЅРѕР·РѕРІ.

    Р’РѕР·РІСЂР°С‰Р°РµС‚ dict СЃ РєР»СЋС‡Р°РјРё:
      - total_evaluated: РєРѕР»-РІРѕ РѕС†РµРЅС‘РЅРЅС‹С… РїСЂРѕРіРЅРѕР·РѕРІ
      - accurate_count:  РєРѕР»-РІРѕ С‚РѕС‡РЅС‹С… (вњ…)
      - accuracy_pct:    % С‚РѕС‡РЅС‹С…
      - mape:            MAPE РїРѕ РґРЅСЏРј (СЃСЂРµРґРЅСЏСЏ Р°Р±СЃ. РѕС€РёР±РєР° / СЃСЂРµРґРЅРµРµ РїСЂРµРґСЃРєР°Р·Р°РЅРёРµ Г— 100)
      - weekly_trend:    list of dicts {week, accurate, total} РґР»СЏ РіСЂР°С„РёРєР°
    """
    empty = {
        'total_evaluated': 0, 'accurate_count': 0,
        'accuracy_pct': 0.0, 'mape': None, 'weekly_trend': [],
    }
    try:
        with db.get_connection() as conn:
            # Р’СЃРµ РїСЂРѕРіРЅРѕР·С‹ РІ С‚РµСЂРјРёРЅР°Р»СЊРЅС‹С… СЃС‚Р°С‚СѓСЃР°С… (РєСЂРѕРјРµ рџ”„ вЂ” РїРµСЂРµСЃС‡РёС‚Р°РЅ)
            terminal = pd.read_sql_query("""
                SELECT id, item_name, predicted_zero_date, created_at, status,
                       avg_daily_sales, lead_time_days
                FROM ai_forecasts
                WHERE status IN (
                    'вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·', 'рџ“‰ РЈРїСѓС‰РµРЅРЅР°СЏ РІС‹РіРѕРґР°', 'рџ”ґ РўРѕРІР°СЂ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚'
                )
                ORDER BY created_at DESC
            """, conn)

            if terminal.empty:
                return empty

            total = len(terminal)
            accurate = (terminal['status'] == 'вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·').sum()
            accuracy_pct = round(accurate / total * 100, 1) if total > 0 else 0.0

            # MAPE: С‚РѕР»СЊРєРѕ РґР»СЏ С‚РѕС‡РЅС‹С… РїСЂРѕРіРЅРѕР·РѕРІ вЂ” СЃСЂР°РІРЅРёРІР°РµРј predicted_zero_date
            # СЃ СЂРµР°Р»СЊРЅРѕР№ РґР°С‚РѕР№ РѕР±РЅСѓР»РµРЅРёСЏ (РµСЃР»Рё РѕРЅР° РёР·РІРµСЃС‚РЅР° вЂ” Р±РµСЂС‘Рј РёР· stocks)
            mape_errors = []
            for _, row in terminal[terminal['status'] == 'вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·'].iterrows():
                pred_date  = pd.to_datetime(row['predicted_zero_date'], errors='coerce')
                created_at = pd.to_datetime(row['created_at'], errors='coerce')
                avg_sales  = float(row['avg_daily_sales'] or 0)
                if pd.isna(pred_date) or pd.isna(created_at) or avg_sales == 0:
                    continue

                # РС‰РµРј С„Р°РєС‚РёС‡РµСЃРєРѕРµ РѕР±РЅСѓР»РµРЅРёРµ РІ stocks
                hist = pd.read_sql_query("""
                    SELECT SUBSTR(report_timestamp, 1, 10) AS date, quantity
                    FROM stocks
                    WHERE item_name = ?
                      AND SUBSTR(report_timestamp, 1, 10) >= ?
                    GROUP BY SUBSTR(report_timestamp, 1, 10)
                    HAVING report_timestamp = MAX(report_timestamp)
                    ORDER BY date ASC
                    LIMIT 30
                """, conn, params=(row['item_name'], created_at.strftime('%Y-%m-%d')))

                threshold = max(1, avg_sales * 0.5)
                zero_rows = hist[hist['quantity'] <= threshold]
                if zero_rows.empty:
                    continue

                actual_zero = pd.to_datetime(zero_rows.iloc[0]['date'])
                predicted_days = (pred_date - created_at).days
                actual_days    = (actual_zero - created_at).days
                if predicted_days > 0:
                    ape = abs(actual_days - predicted_days) / predicted_days * 100
                    mape_errors.append(ape)

            mape = round(sum(mape_errors) / len(mape_errors), 1) if mape_errors else None

            # РќРµРґРµР»СЊРЅС‹Р№ С‚СЂРµРЅРґ
            terminal['week'] = pd.to_datetime(
                terminal['created_at'], errors='coerce'
            ).dt.to_period('W').astype(str)

            weekly = (
                terminal.groupby('week')
                .apply(lambda g: pd.Series({
                    'total':    len(g),
                    'accurate': (g['status'] == 'вњ… РўРѕС‡РЅС‹Р№ РїСЂРѕРіРЅРѕР·').sum(),
                }))
                .reset_index()
                .sort_values('week')
                .tail(12)  # РїРѕСЃР»РµРґРЅРёРµ 12 РЅРµРґРµР»СЊ
            )
            weekly_trend = weekly.to_dict('records')

            return {
                'total_evaluated': int(total),
                'accurate_count':  int(accurate),
                'accuracy_pct':    accuracy_pct,
                'mape':            mape,
                'weekly_trend':    weekly_trend,
            }

    except Exception:
        logger.exception('_load_accuracy_stats error')
        return empty


# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
#  РЎС‚СЂР°РЅРёС†Р°
# в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def setup_page():

    @ui.page('/abtest')
    async def abtest_page():
        logger.info('abtest_page() handler entered')
        build_shell('/abtest')

        with ui.column().classes('w-full p-4 gap-6').style(
            'background:#0d0d0d; min-height:100vh;'
        ):
            # в”Ђв”Ђ Р—Р°РіРѕР»РѕРІРѕРє в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
            ui.label('вљ–пёЏ A/B РўРµСЃС‚: AI-РїСЂРѕРіРЅРѕР· vs Р§РµР»РѕРІРµС‡РµСЃРєРёРµ СЂРµС€РµРЅРёСЏ').classes(
                'text-white text-2xl font-bold'
            )
            ui.label(
                'РўРµРЅРµРІРѕР№ СЂРµР¶РёРј: Р°Р»РіРѕСЂРёС‚Рј РґРµР»Р°РµС‚ РїСЂРѕРіРЅРѕР·С‹ Р·Р°РєСѓРїРѕРє Рё СЃРІРµСЂСЏРµС‚ РёС… '
                'СЃ СЂРµР°Р»СЊРЅС‹РјРё РґРµР№СЃС‚РІРёСЏРјРё РјРµРЅРµРґР¶РµСЂРѕРІ. РџРѕР·РІРѕР»СЏРµС‚ РѕС†РµРЅРёС‚СЊ СѓРїСѓС‰РµРЅРЅСѓСЋ '
                'РІС‹РіРѕРґСѓ Р±РµР· РІРјРµС€Р°С‚РµР»СЊСЃС‚РІР° РІ Р±РёР·РЅРµСЃ-РїСЂРѕС†РµСЃСЃС‹.'
            ).style('color:#9ca3af; font-size:0.85rem;')

            ui.separator().style('background:#2a2a2a;')

            # в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
            # РћСЃРЅРѕРІРЅРѕР№ refreshable
            # в•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђв•ђ
            @ui.refreshable
            async def render_main():

                # в”Ђв”Ђ Cold Start РёРЅРґРёРєР°С‚РѕСЂ в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                days = await ng_run.io_bound(_days_in_db)
                if days < 30:
                    with ui.card().classes('w-full p-4').style(
                        'background:#1c1917; border:1px solid #a16207;'
                    ):
                        ui.label(
                            f'вљ пёЏ РњРѕРґРµР»СЊ РІ СЃС‚Р°РґРёРё В«РїСЂРѕРіСЂРµРІР°В» (Cold Start): '
                            f'РЅР°РєРѕРїР»РµРЅРѕ {days} РёР· 30 РЅРµРѕР±С…РѕРґРёРјС‹С… РґРЅРµР№. '
                            'РР СЌРєСЃС‚СЂР°РїРѕР»РёСЂСѓРµС‚ РєРѕСЂРѕС‚РєРёРµ С‚СЂРµРЅРґС‹ вЂ” РІРѕР·РјРѕР¶РЅР° РїРѕРІС‹С€РµРЅРЅР°СЏ РїРѕРіСЂРµС€РЅРѕСЃС‚СЊ.'
                        ).classes('text-yellow-300 text-sm')
                else:
                    with ui.card().classes('w-full p-4').style(
                        'background:#052e16; border:1px solid #22c55e;'
                    ):
                        ui.label(
                            f'вњ… РњРѕРґРµР»СЊ РѕР±СѓС‡РµРЅР°: РЅР°РєРѕРїР»РµРЅРѕ РґР°РЅРЅС‹С… Р·Р° {days} РґРЅРµР№. '
                            'РўРѕС‡РЅРѕСЃС‚СЊ РїСЂРѕРіРЅРѕР·РѕРІ РѕРїС‚РёРјР°Р»СЊРЅР°.'
                        ).classes('text-green-400 text-sm')

                # в”Ђв”Ђ РћР±РЅРѕРІР»СЏРµРј СЃС‚Р°С‚СѓСЃС‹ Рё РїСЂРѕРІРµСЂСЏРµРј С‚РѕС‡РЅРѕСЃС‚СЊ РїСЂРѕРіРЅРѕР·РѕРІ в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                await ng_run.io_bound(_verify_shadow_forecasts)
                await ng_run.io_bound(_check_forecast_accuracy)
                acc_stats = await ng_run.io_bound(_load_accuracy_stats)
                df_fc = await ng_run.io_bound(_load_forecasts)

                # в”Ђв”Ђ РќРµС‚ РїСЂРѕРіРЅРѕР·РѕРІ в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                if df_fc.empty:
                    with ui.card().classes('w-full p-4').style(
                        'background:#111111; border:1px solid #2a2a2a;'
                    ):
                        ui.label(
                            'в„№пёЏ РџРѕРєР° РЅРµС‚ Р°РєС‚РёРІРЅС‹С… РїСЂРѕРіРЅРѕР·РѕРІ. '
                            'РќР°Р¶РјРёС‚Рµ РєРЅРѕРїРєСѓ РЅРёР¶Рµ, С‡С‚РѕР±С‹ Р·Р°РїСѓСЃС‚РёС‚СЊ AI-Р°РЅР°Р»РёР·.'
                        ).classes('text-gray-400')
                else:
                    # в”Ђв”Ђ РњРµС‚СЂРёРєРё (СѓРїСѓС‰РµРЅРЅР°СЏ РІС‹РіРѕРґР° + Р·Р°РјРѕСЂРѕР·РєР°) в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                    total_lost      = float(df_fc['lost_sales_value'].fillna(0).sum())
                    total_overstock = float(df_fc['overstock_value'].fillna(0).sum())

                    with ui.row().classes('gap-4 flex-wrap'):
                        with ui.card().classes('p-5').style(
                            'background:#171717; border-left:3px solid #ef4444;'
                        ):
                            ui.label(
                                f"{total_lost:,.0f} в‚Ѕ".replace(',', '\u202f')
                            ).classes('text-white text-2xl font-bold')
                            ui.label('рџ“‰ РЈРїСѓС‰РµРЅРЅР°СЏ РІС‹РіРѕРґР° (Prevented Lost Sales)').style(
                                'color:#9ca3af; font-size:0.8rem;'
                            )
                            ui.label(
                                'РЎСѓРјРјР° РїРѕС‚РµСЂСЊ РёР·-Р·Р° РЅРµСЃРІРѕРµРІСЂРµРјРµРЅРЅС‹С… Р·Р°РєСѓРїРѕРє'
                            ).style('color:#6b7280; font-size:0.72rem;')

                        with ui.card().classes('p-5').style(
                            'background:#171717; border-left:3px solid #38bdf8;'
                        ):
                            ui.label(
                                f"{total_overstock:,.0f} в‚Ѕ".replace(',', '\u202f')
                            ).classes('text-white text-2xl font-bold')
                            ui.label('рџ§Љ Р—Р°РјРѕСЂРѕР¶РµРЅРЅС‹Р№ РєР°РїРёС‚Р°Р» (Cost of Overstock)').style(
                                'color:#9ca3af; font-size:0.8rem;'
                            )
                            ui.label(
                                'РР·Р»РёС€РєРё, РєСѓРїР»РµРЅРЅС‹Рµ СЃРІРµСЂС… СЂРµРєРѕРјРµРЅРґР°С†РёР№ РР'
                            ).style('color:#6b7280; font-size:0.72rem;')

                    ui.separator().style('background:#2a2a2a;')

                    # в”Ђв”Ђ Accuracy Dashboard в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                    ui.label('рџЋЇ РўРѕС‡РЅРѕСЃС‚СЊ РїСЂРѕРіРЅРѕР·РѕРІ (Accuracy Dashboard)').classes(
                        'text-white text-lg font-semibold'
                    )

                    acc_total    = acc_stats['total_evaluated']
                    acc_accurate = acc_stats['accurate_count']
                    acc_pct      = acc_stats['accuracy_pct']
                    acc_mape     = acc_stats['mape']
                    acc_trend    = acc_stats['weekly_trend']

                    if acc_total < 3:
                        with ui.card().classes('w-full p-3').style(
                            'background:#111827; border:1px dashed #374151;'
                        ):
                            ui.label(
                                'вЏі Р”Р°РЅРЅС‹С… РїРѕРєР° РЅРµРґРѕСЃС‚Р°С‚РѕС‡РЅРѕ РґР»СЏ РѕС†РµРЅРєРё С‚РѕС‡РЅРѕСЃС‚Рё. '
                                f'РћС†РµРЅРµРЅРѕ РїСЂРѕРіРЅРѕР·РѕРІ: {acc_total}. '
                                f'РќСѓР¶РЅРѕ РјРёРЅРёРјСѓРј 3 Р·Р°РІРµСЂС€С‘РЅРЅС‹С… РїСЂРѕРіРЅРѕР·Р° вЂ” СЃРёСЃС‚РµРјР° РЅР°РєР°РїР»РёРІР°РµС‚ РёСЃС‚РѕСЂРёСЋ.'
                            ).style('color:#6b7280; font-size:0.85rem;')
                    else:
                        # РљР°СЂС‚РѕС‡РєРё С‚РѕС‡РЅРѕСЃС‚Рё
                        with ui.row().classes('gap-4 flex-wrap w-full'):
                            # РўРѕС‡РЅРѕСЃС‚СЊ %
                            acc_color = (
                                '#22c55e' if acc_pct >= 70
                                else '#f59e0b' if acc_pct >= 40
                                else '#ef4444'
                            )
                            with ui.card().classes('p-5').style(
                                f'background:#171717; border-left:3px solid {acc_color};'
                            ):
                                ui.label(f'{acc_pct:.1f}%').classes(
                                    'text-white text-2xl font-bold'
                                )
                                ui.label('рџЋЇ РўРѕС‡РЅРѕСЃС‚СЊ (Forecast Accuracy)').style(
                                    'color:#9ca3af; font-size:0.8rem;'
                                )
                                ui.label(
                                    '% РїСЂРѕРіРЅРѕР·РѕРІ, РїРѕРїР°РІС€РёС… РІ В±3 РґРЅСЏ РѕС‚ С„Р°РєС‚Р°'
                                ).style('color:#6b7280; font-size:0.72rem;')

                            # MAPE
                            mape_txt = f'{acc_mape:.1f}%' if acc_mape is not None else 'вЂ”'
                            mape_color = (
                                '#22c55e' if acc_mape is not None and acc_mape < 15
                                else '#f59e0b' if acc_mape is not None and acc_mape < 35
                                else '#ef4444'
                            )
                            with ui.card().classes('p-5').style(
                                f'background:#171717; border-left:3px solid {mape_color};'
                            ):
                                ui.label(mape_txt).classes(
                                    'text-white text-2xl font-bold'
                                )
                                ui.label('рџ“ђ РћС€РёР±РєР° РїСЂРѕРіРЅРѕР·Р° (MAPE)').style(
                                    'color:#9ca3af; font-size:0.8rem;'
                                )
                                ui.label(
                                    'РЎСЂРµРґРЅ. % РѕС‚РєР»РѕРЅРµРЅРёСЏ РѕС‚ С„Р°РєС‚РёС‡РµСЃРєРѕР№ РґР°С‚С‹'
                                ).style('color:#6b7280; font-size:0.72rem;')

                            # РћС†РµРЅРµРЅРѕ / С‚РѕС‡РЅС‹С…
                            with ui.card().classes('p-5').style(
                                'background:#171717; border-left:3px solid #818cf8;'
                            ):
                                ui.label(f'{acc_accurate} / {acc_total}').classes(
                                    'text-white text-2xl font-bold'
                                )
                                ui.label('рџ“Љ РўРѕС‡РЅС‹С… / РћС†РµРЅРµРЅРѕ РїСЂРѕРіРЅРѕР·РѕРІ').style(
                                    'color:#9ca3af; font-size:0.8rem;'
                                )
                                ui.label(
                                    'РќР°РєРѕРїР»РµРЅРЅР°СЏ РёСЃС‚РѕСЂРёСЏ РІРµСЂРёС„РёРєР°С†РёРё'
                                ).style('color:#6b7280; font-size:0.72rem;')

                        # РќРµРґРµР»СЊРЅС‹Р№ С‚СЂРµРЅРґ (СЃС‚РµРєРѕРІР°СЏ РіРёСЃС‚РѕРіСЂР°РјРјР°)
                        if acc_trend:
                            weeks      = [r['week'] for r in acc_trend]
                            acc_vals   = [int(r['accurate']) for r in acc_trend]
                            inac_vals  = [int(r['total']) - int(r['accurate']) for r in acc_trend]

                            ui.echart({
                                'backgroundColor': 'transparent',
                                'tooltip': {'trigger': 'axis', 'axisPointer': {'type': 'shadow'}},
                                'legend': {
                                    'data': ['вњ… РўРѕС‡РЅС‹Рµ', 'вќЊ РќРµС‚РѕС‡РЅС‹Рµ'],
                                    'textStyle': {'color': '#9ca3af'},
                                },
                                'grid': {'left': '3%', 'right': '4%', 'bottom': '3%', 'containLabel': True},
                                'xAxis': {
                                    'type': 'category', 'data': weeks,
                                    'axisLabel': {'color': '#6b7280', 'rotate': 30, 'fontSize': 10},
                                    'axisLine': {'lineStyle': {'color': '#374151'}},
                                },
                                'yAxis': {
                                    'type': 'value', 'minInterval': 1,
                                    'axisLabel': {'color': '#6b7280'},
                                    'splitLine': {'lineStyle': {'color': '#1f2937'}},
                                },
                                'series': [
                                    {
                                        'name': 'вњ… РўРѕС‡РЅС‹Рµ',
                                        'type': 'bar', 'stack': 'total',
                                        'data': acc_vals,
                                        'itemStyle': {'color': '#22c55e'},
                                        'label': {'show': True, 'position': 'inside', 'color': '#fff', 'fontSize': 10},
                                    },
                                    {
                                        'name': 'вќЊ РќРµС‚РѕС‡РЅС‹Рµ',
                                        'type': 'bar', 'stack': 'total',
                                        'data': inac_vals,
                                        'itemStyle': {'color': '#374151'},
                                        'label': {'show': True, 'position': 'inside', 'color': '#9ca3af', 'fontSize': 10},
                                    },
                                ],
                            }).classes('w-full').style('height:220px;')

                    ui.separator().style('background:#2a2a2a;')

                    # в”Ђв”Ђ Р”РµС‚Р°Р»РёР·Р°С†РёСЏ вЂ” Р¶СѓСЂРЅР°Р» РїСЂРѕРіРЅРѕР·РѕРІ в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                    ui.label('Р”РµС‚Р°Р»РёР·Р°С†РёСЏ (Р–СѓСЂРЅР°Р» РїСЂРѕРіРЅРѕР·РѕРІ Рё С„РёРЅР°РЅСЃРѕРІС‹С… РїРѕСЃР»РµРґСЃС‚РІРёР№):').classes(
                        'text-white text-lg font-semibold'
                    )

                    disp = df_fc[[
                        'created_at', 'item_name', 'current_qty',
                        'predicted_zero_date', 'recommended_qty',
                        'avg_daily_sales', 'lead_time_days', 'safety_stock',
                        'reason', 'status',
                        'lost_sales_value', 'overstock_value',
                        'sparkline',
                        'abc_category',
                    ]].copy() if 'abc_category' in df_fc.columns else df_fc[[
                        'created_at', 'item_name', 'current_qty',
                        'predicted_zero_date', 'recommended_qty',
                        'avg_daily_sales', 'lead_time_days', 'safety_stock',
                        'reason', 'status',
                        'lost_sales_value', 'overstock_value',
                        'sparkline',
                    ]].copy().assign(abc_category='?')

                    disp['current_qty'] = disp['current_qty'].fillna(0).astype(int)
                    disp['created_at']  = disp['created_at'].astype(str).str[:10]
                    disp['abc_category'] = disp['abc_category'].fillna('C')
                    disp['lost_sales_value']  = disp['lost_sales_value'].fillna(0)
                    disp['overstock_value']   = disp['overstock_value'].fillna(0)
                    disp['РЈРїСѓС‰. РІС‹СЂСѓС‡РєР° (в‚Ѕ)'] = disp['lost_sales_value'].apply(_fmt_rub)
                    disp['Р—Р°РјРѕСЂРѕР¶РµРЅРѕ (в‚Ѕ)']    = disp['overstock_value'].apply(_fmt_rub)
                    # sparkline: РїСЂРµРѕР±СЂР°Р·СѓРµРј РІ СЃРїРёСЃРѕРє С‡РёСЃРµР» (РЅР° СЃР»СѓС‡Р°Р№ РµСЃР»Рё РїСЂРёС€Р»Рё NaN)
                    disp['sparkline'] = disp['sparkline'].apply(
                        lambda v: [int(x) for x in v] if isinstance(v, list) else []
                    )
                    disp = disp.drop(columns=['lost_sales_value', 'overstock_value'])
                    disp = disp.rename(columns={
                        'created_at':           'Р”Р°С‚Р°',
                        'item_name':            'РўРѕРІР°СЂ',
                        'current_qty':          'РћСЃС‚Р°С‚РѕРє',
                        'predicted_zero_date':  'РћР±РЅСѓР»РёС‚СЃСЏ',
                        'recommended_qty':      'Р—Р°РєР°Р· (С€С‚)',
                        'avg_daily_sales':      'Р Р°СЃС…РѕРґ/РґРµРЅСЊ',
                        'lead_time_days':       'РЎСЂРѕРє РїРѕСЃС‚.',
                        'safety_stock':         'РЎС‚СЂР°С…. Р·Р°РїР°СЃ',
                        'reason':               'Р”РёР°РіРЅРѕР·',
                        'status':               'РЎС‚Р°С‚СѓСЃ',
                        'abc_category':         'ABC',
                    })

                    col_defs = [
                        {'field': 'Р”Р°С‚Р°',  'headerName': 'Р”Р°С‚Р°', 'flex': 1, 'sortable': True},
                        {
                            'field': 'ABC', 'headerName': 'ABC', 'flex': 1,
                            'sortable': True,
                            'cellStyle': {
                                'function': (
                                    "const v=params.value||'';"
                                    "if(v==='A')return{color:'#fbbf24',fontWeight:'800',fontSize:'1rem',textAlign:'center'};"
                                    "if(v==='B')return{color:'#38bdf8',fontWeight:'700',textAlign:'center'};"
                                    "return{color:'#6b7280',textAlign:'center'};"
                                )
                            },
                            'headerTooltip': 'A вЂ” РєСЂРёС‚РёС‡РµСЃРєРё РІР°Р¶РЅС‹Рµ (80% РѕР±РѕСЂРѕС‚Р°), B вЂ” СѓРјРµСЂРµРЅРЅРѕ РІР°Р¶РЅС‹Рµ (15%), C вЂ” РЅРёР·РєРѕРїСЂРёРѕСЂРёС‚РµС‚РЅС‹Рµ (5%)',
                        },
                        {'field': 'РўРѕРІР°СЂ', 'headerName': 'РўРѕРІР°СЂ', 'flex': 3, 'sortable': True, 'filter': True, 'resizable': True},
                        {'field': 'РћСЃС‚Р°С‚РѕРє', 'headerName': 'РћСЃС‚Р°С‚РѕРє', 'flex': 1, 'type': 'numericColumn'},
                        {
                            'field': 'sparkline',
                            'headerName': 'РўСЂРµРЅРґ (30Рґ)',
                            'flex': 2,
                            'cellRenderer': 'agSparklineCellRenderer',
                            'cellRendererParams': {
                                'sparklineOptions': {
                                    'type': 'line',
                                    'line': {'stroke': '#22c55e', 'strokeWidth': 1.5},
                                    'marker': {'enabled': False},
                                    'crosshairs': {
                                        'xLine': {'enabled': True, 'lineDash': 'dash', 'stroke': '#374151'},
                                        'yLine': {'enabled': True, 'lineDash': 'dash', 'stroke': '#374151'},
                                    },
                                    'tooltip': {
                                        'enabled': True,
                                        'renderer': 'function(params){return {title:"",content:params.yValue+" С€С‚"}}',
                                    },
                                    'fill': 'rgba(34,197,94,0.08)',
                                    'padding': {'top': 6, 'bottom': 6},
                                },
                            },
                        },
                        {'field': 'РћР±РЅСѓР»РёС‚СЃСЏ',  'headerName': 'РћР±РЅСѓР»РёС‚СЃСЏ',   'flex': 1,  'sortable': True},
                        {'field': 'Р—Р°РєР°Р· (С€С‚)', 'headerName': 'Р—Р°РєР°Р· (С€С‚)', 'flex': 1,  'type': 'numericColumn'},
                        {'field': 'Р Р°СЃС…РѕРґ/РґРµРЅСЊ','headerName': 'Р Р°СЃС…РѕРґ/Рґ',    'flex': 1,  'type': 'numericColumn'},
                        {'field': 'РЎСЂРѕРє РїРѕСЃС‚.', 'headerName': 'РЎСЂРѕРє РїРѕСЃС‚.', 'flex': 1},
                        {'field': 'РЎС‚СЂР°С…. Р·Р°РїР°СЃ','headerName': 'РЎС‚СЂР°С…. Р·Р°Рї.','flex': 1, 'type': 'numericColumn'},
                        {
                            'field': 'РЎС‚Р°С‚СѓСЃ', 'headerName': 'РЎС‚Р°С‚СѓСЃ', 'flex': 2,
                            'cellStyle': {
                                'function': (
                                    "const s=params.value||'';"
                                    "if(s.includes('рџ“‰')||s.includes('рџ”ґ'))return{color:'#ef4444',fontWeight:'600'};"
                                    "if(s.includes('рџ§Љ'))return{color:'#38bdf8',fontWeight:'600'};"
                                    "if(s.includes('вњ…'))return{color:'#22c55e',fontWeight:'600'};"
                                    "if(s.includes('вЏі')||s.includes('рџ”„'))return{color:'#f59e0b'};"
                                    "return{color:'#9ca3af'};"
                                )
                            },
                        },
                        {'field': 'РЈРїСѓС‰. РІС‹СЂСѓС‡РєР° (в‚Ѕ)', 'headerName': 'РЈРїСѓС‰. РІС‹СЂСѓС‡РєР°',
                         'flex': 1, 'cellStyle': {'color': '#ef4444', 'fontWeight': '600'}},
                        {'field': 'Р—Р°РјРѕСЂРѕР¶РµРЅРѕ (в‚Ѕ)',    'headerName': 'Р—Р°РјРѕСЂРѕР¶РµРЅРѕ',
                         'flex': 1, 'cellStyle': {'color': '#38bdf8'}},
                        {'field': 'Р”РёР°РіРЅРѕР·', 'headerName': 'Р”РёР°РіРЅРѕР· (AI)',
                         'flex': 4, 'resizable': True,
                         'cellStyle': {'color': '#d1d5db', 'fontSize': '0.8rem', 'whiteSpace': 'normal', 'lineHeight': '1.4'}},
                    ]

                    ui.aggrid({
                        'columnDefs':         col_defs,
                        'rowData':            disp.to_dict('records'),
                        'defaultColDef':      {'resizable': True},
                        'rowHeight':          60,
                        'pagination':         True,
                        'paginationPageSize': 15,
                    }).classes('w-full ag-theme-balham-dark').style('height:500px;')

                ui.separator().style('background:#2a2a2a;')

                # в”Ђв”Ђ РЎС‚Р°С‚СѓСЃ Р°РІС‚РѕРјР°С‚РёР·Р°С†РёРё в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                ui.label('вљ™пёЏ РЈРїСЂР°РІР»РµРЅРёРµ AI-Р°РЅР°Р»РёР·РѕРј').classes(
                    'text-white text-lg font-semibold'
                )

                has_pending = _AI_PENDING_FLAG.exists()
                today_count = _forecasts_today()

                # Р§РёС‚Р°РµРј РґР°С‚Сѓ РїРѕСЃР»РµРґРЅРµРіРѕ РїР°СЂСЃРёРЅРіР°
                try:
                    _last_run_cfg = db.CONFIG.get('paths', {})
                    _base_dir = Path(__file__).resolve().parent.parent.parent
                    _last_run_path = _base_dir / _last_run_cfg.get('last_run_file', 'logs/last_run.txt')
                    last_parse_date = _last_run_path.read_text(encoding='utf-8').strip() \
                        if _last_run_path.exists() else None
                except Exception:
                    last_parse_date = None

                with ui.row().classes('gap-4 flex-wrap w-full'):
                    # в”Ђв”Ђ Р‘Р»РѕРє В«РђРІС‚РѕРјР°С‚РёС‡РµСЃРєРёР№ СЂРµР¶РёРјВ» в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                    with ui.card().classes('p-4 flex-1').style(
                        'background:#111827; border:1px solid #1f2937; min-width:280px;'
                    ):
                        ui.label('рџ¤– РђРІС‚РѕРјР°С‚РёС‡РµСЃРєРёР№ СЂРµР¶РёРј').classes(
                            'text-white font-semibold mb-2'
                        )
                        ui.label(
                            'РџРѕСЃР»Рµ РєР°Р¶РґРѕРіРѕ СѓСЃРїРµС€РЅРѕРіРѕ РїР°СЂСЃРёРЅРіР° autostart.py '
                            'Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё Р·Р°РїСѓСЃРєР°РµС‚ src/ai_services.py вЂ” '
                            'РїСЂРѕРіРЅРѕР·С‹ РѕР±РЅРѕРІР»СЏСЋС‚СЃСЏ Р±РµР· СѓС‡Р°СЃС‚РёСЏ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ.'
                        ).style('color:#6b7280; font-size:0.8rem;')

                        ui.separator().style('background:#1f2937; margin:8px 0;')

                        if last_parse_date:
                            ui.label(f'рџ“… РџРѕСЃР»РµРґРЅРёР№ РїР°СЂСЃРёРЅРі: {last_parse_date}').style(
                                'color:#9ca3af; font-size:0.82rem;'
                            )

                        if has_pending:
                            # РџР°СЂСЃРµСЂ РѕС‚СЂР°Р±РѕС‚Р°Р», РЅРѕ ai_forecaster РЅРµ СѓСЃРїРµР»
                            with ui.row().classes('items-center gap-2 mt-1'):
                                ui.icon('warning', size='sm').style('color:#f59e0b;')
                                ui.label(
                                    'РџР°СЂСЃРµСЂ СЃРѕР±СЂР°Р» РґР°РЅРЅС‹Рµ, РЅРѕ AI-Р°РЅР°Р»РёР· РµС‰С‘ РЅРµ РІС‹РїРѕР»РЅРµРЅ. '
                                    'Р’РѕР·РјРѕР¶РЅРѕ, ai_forecaster.py СѓРїР°Р» вЂ” Р·Р°РїСѓСЃС‚РёС‚Рµ РІСЂСѓС‡РЅСѓСЋ.'
                                ).style('color:#fbbf24; font-size:0.82rem;')
                        elif today_count > 0:
                            with ui.row().classes('items-center gap-2 mt-1'):
                                ui.icon('check_circle', size='sm').style('color:#22c55e;')
                                ui.label(
                                    f'РђРІС‚РѕР°РЅР°Р»РёР· РІС‹РїРѕР»РЅРµРЅ СЃРµРіРѕРґРЅСЏ: {today_count} РїСЂРѕРіРЅРѕР·РѕРІ РІ Р±Р°Р·Рµ.'
                                ).style('color:#86efac; font-size:0.82rem;')
                        else:
                            with ui.row().classes('items-center gap-2 mt-1'):
                                ui.icon('schedule', size='sm').style('color:#6b7280;')
                                ui.label(
                                    'РђРЅР°Р»РёР· СЃРµРіРѕРґРЅСЏ РµС‰С‘ РЅРµ Р·Р°РїСѓСЃРєР°Р»СЃСЏ. '
                                    'Р–РґС‘Рј СЃР»РµРґСѓСЋС‰РµРіРѕ РїР°СЂСЃРёРЅРіР°.'
                                ).style('color:#6b7280; font-size:0.82rem;')

                    # в”Ђв”Ђ Р‘Р»РѕРє В«Р СѓС‡РЅРѕР№ Р·Р°РїСѓСЃРєВ» в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ
                    with ui.card().classes('p-4 flex-1').style(
                        'background:#111827; border:1px solid #1f2937; min-width:280px;'
                    ):
                        ui.label('рџ–±пёЏ Р СѓС‡РЅРѕР№ Р·Р°РїСѓСЃРє').classes(
                            'text-white font-semibold mb-2'
                        )
                        ui.label(
                            'РСЃРїРѕР»СЊР·СѓР№С‚Рµ РµСЃР»Рё: AI-СЃРєСЂРёРїС‚ СѓРїР°Р» Р°РІС‚РѕРјР°С‚РёС‡РµСЃРєРё, '
                            'С…РѕС‚РёС‚Рµ РїРµСЂРµСЃС‡РёС‚Р°С‚СЊ РїСЂРѕРіРЅРѕР·С‹ СЃ РЅРѕРІС‹РјРё РїР°СЂР°РјРµС‚СЂР°РјРё, '
                            'РёР»Рё РїСЂРѕСЃС‚Рѕ РїСЂРѕРІРµСЂРёС‚СЊ СЃРёСЃС‚РµРјСѓ.'
                        ).style('color:#6b7280; font-size:0.8rem;')

                        ui.separator().style('background:#1f2937; margin:8px 0;')

                        status_lbl = ui.label('').style(
                            'color:#818cf8; font-weight:600; font-size:0.85rem;'
                        )
                        status_lbl.set_visibility(False)

                        async def do_forecast():
                            forecast_btn.set_enabled(False)
                            status_lbl.set_text('рџ¤– РР Р°РЅР°Р»РёР·РёСЂСѓРµС‚ РіСЂР°С„РёРєРё РїСЂРѕРґР°Р¶вЂ¦')
                            status_lbl.set_visibility(True)
                            try:
                                result = await ng_run.io_bound(ai_services.run_batch_forecast)

                                if result == 'no_key':
                                    ui.notify(
                                        'вќЊ API-РєР»СЋС‡ РЅРµ РЅР°Р№РґРµРЅ! РџСЂРѕРІРµСЂСЊС‚Рµ secrets.toml.',
                                        type='negative', timeout=0
                                    )
                                elif result == 'empty':
                                    ui.notify(
                                        'вљ пёЏ РќРµС‚ С‚РѕРІР°СЂРѕРІ РґР»СЏ Р°РЅР°Р»РёР·Р° вЂ” '
                                        'РЅРµС‚ СЃРЅРёР¶РµРЅРёР№ РѕСЃС‚Р°С‚РєРѕРІ Р·Р° РїРѕСЃР»РµРґРЅРёРµ 30 РґРЅРµР№.',
                                        type='warning'
                                    )
                                    if _AI_PENDING_FLAG.exists():
                                        _AI_PENDING_FLAG.unlink()
                                elif isinstance(result, str) and result.startswith('error_'):
                                    err = result.split('_', 1)[1]
                                    ui.notify(
                                        f'вќЊ РћС€РёР±РєР° AI: {err}',
                                        type='negative', timeout=0
                                    )
                                elif isinstance(result, str) and result.startswith('ok_'):
                                    count = result.split('_', 1)[1]
                                    ui.notify(
                                        f'вњ… Р“РѕС‚РѕРІРѕ! РЎРіРµРЅРµСЂРёСЂРѕРІР°РЅРѕ РїСЂРѕРіРЅРѕР·РѕРІ: {count}.',
                                        type='positive'
                                    )
                                    if _AI_PENDING_FLAG.exists():
                                        _AI_PENDING_FLAG.unlink()
                                    await render_main.refresh()
                                else:
                                    ui.notify(f'Р РµР·СѓР»СЊС‚Р°С‚: {result}', type='info')

                            except Exception as ex:
                                logger.exception('run_batch_forecast error')
                                ui.notify(
                                    f'вќЊ РљСЂРёС‚РёС‡РµСЃРєР°СЏ РѕС€РёР±РєР°: {ex}',
                                    type='negative', timeout=0
                                )
                            finally:
                                forecast_btn.set_enabled(True)
                                status_lbl.set_visibility(False)

                        btn_label = (
                            'рџљЂ Р—Р°РїСѓСЃС‚РёС‚СЊ Р°РЅР°Р»РёР· (pending РґР°РЅРЅС‹Рµ)'
                            if has_pending else
                            'рџ”„ РџСЂРёРЅСѓРґРёС‚РµР»СЊРЅС‹Р№ РїРµСЂРµСЃС‡С‘С‚'
                            if today_count > 0 else
                            'рџљЂ Р—Р°РїСѓСЃС‚РёС‚СЊ РїРµСЂРІРёС‡РЅС‹Р№ Р°РЅР°Р»РёР·'
                        )
                        btn_color = 'primary' if has_pending or today_count == 0 else 'secondary'

                        forecast_btn = ui.button(btn_label, on_click=do_forecast) \
                            .props(f'color={btn_color} no-caps') \
                            .classes('w-full mt-1')
                        status_lbl  # rendered after button


            # ── Уведомления об ошибках LLM ───────────────────────────────────
            _llm_err_log = BASE_DIR / 'logs' / 'llm_errors.log'
            if _llm_err_log.exists():
                try:
                    lines = _llm_err_log.read_text(encoding='utf-8').strip().splitlines()
                    from datetime import datetime, timedelta
                    cutoff = datetime.now() - timedelta(hours=24)
                    recent = []
                    for ln in reversed(lines):
                        try:
                            ts_str = ln.split(' | ')[0]
                            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                            if ts >= cutoff:
                                recent.append(ln)
                        except Exception:
                            pass
                        if len(recent) >= 3:
                            break
                    if recent:
                        ui.separator().style('background:#2a2a2a; margin-top:16px;')
                        with ui.expansion(
                            f'⚠️ Ошибки LLM за последние 24ч ({len(recent)} шт.)',
                            icon='warning',
                        ).classes('w-full text-yellow-400 mt-2'):
                            for err_line in recent:
                                parts = err_line.split(' | ', 1)
                                ts_part  = parts[0] if len(parts) > 1 else ''
                                msg_part = parts[1] if len(parts) > 1 else err_line
                                with ui.row().classes('gap-2 items-start w-full'):
                                    ui.label(ts_part).classes('text-xs text-gray-500 shrink-0')
                                    ui.label(msg_part).classes('text-xs text-yellow-300 break-all')
                except Exception:
                    pass


            await render_main()



