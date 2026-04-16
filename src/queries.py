def get_anomalies_query() -> str:
    return """
        SELECT 
            sku, 
            item_name,
            SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :yesterday THEN quantity ELSE 0 END) as qty_old,
            SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :today THEN quantity ELSE 0 END) as qty_new,
            (SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :today THEN quantity ELSE 0 END) - 
             SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :yesterday THEN quantity ELSE 0 END)) as delta,
            
            -- Проверяем наличие истории ДО вчерашнего дня
            (SELECT COUNT(*) FROM stocks s_hist 
             WHERE s_hist.item_name = stocks.item_name 
             AND SUBSTR(s_hist.report_timestamp, 1, 10) < :yesterday) as history_count,
             
            -- Проверяем, встречался ли такой артикул с другим названием (поиск переименования)
            (SELECT item_name FROM stocks s_sku 
             WHERE s_sku.sku = stocks.sku 
             AND s_sku.item_name != stocks.item_name 
             LIMIT 1) as old_name_alias,

            -- Проверяем, встречалось ли такое имя с другим артикулом
            (SELECT sku FROM stocks s_name
             WHERE s_name.item_name = stocks.item_name 
             AND s_name.sku != stocks.sku AND s_name.sku != ''
             LIMIT 1) as old_sku_alias
             
        FROM stocks
        WHERE (SUBSTR(report_timestamp, 1, 10) = :today OR SUBSTR(report_timestamp, 1, 10) = :yesterday)
          AND item_name IS NOT NULL
        GROUP BY item_name, sku
        HAVING delta > 0
    """

def get_insert_anomaly_query() -> str:
    """Запрос для фиксации аномалии с учетом комментария"""
    return """
        INSERT INTO anomaly_log (
            detected_at, item_name, anomaly_type, 
            qty_system, qty_physical, financial_impact, 
            source, status, comment
        ) VALUES (
            datetime('now', 'localtime'), :item_name, :anomaly_type, 
            :qty_system, :qty_physical, :financial_impact, 
            :source, :status, :comment
        )
    """

def get_cancel_anomaly_query() -> str:
    """Помечает задачу как отмененную и сохраняет причину отмены"""
    return """
        UPDATE anomaly_log 
        SET status = 'Отменена', 
            resolved_at = datetime('now', 'localtime'),
            comment = :comment 
        WHERE id = :id
    """

def get_close_anomaly_query() -> str:
    """Помечает задачу как закрытую и сохраняет итоговый комментарий"""
    return """
        UPDATE anomaly_log 
        SET status = 'Закрыта', 
            resolved_at = datetime('now', 'localtime'),
            comment = :comment
    WHERE id = :id
    """

def get_sla_metrics_query(sla_hours=4) -> str:
    """
    Считает процент соблюдения SLA (задачи, закрытые быстрее чем за N часов).
    """
    return f"""
        SELECT 
            COUNT(*) as total_resolved,
            SUM(CASE 
                WHEN (julianday(resolved_at) - julianday(detected_at)) * 24 <= {sla_hours} 
                THEN 1 ELSE 0 
            END) as within_sla
        FROM anomaly_log
        WHERE resolved_at IS NOT NULL 
          AND status != 'Отменена'
    """