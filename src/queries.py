def get_anomalies_query() -> str:
    return """
        SELECT 
            sku, 
            item_name,
            SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :yesterday THEN quantity ELSE 0 END) as qty_old,
            SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :today THEN quantity ELSE 0 END) as qty_new,
            (SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :today THEN quantity ELSE 0 END) - 
             SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :yesterday THEN quantity ELSE 0 END)) as delta
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