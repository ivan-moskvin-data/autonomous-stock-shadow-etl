def get_anomalies_query() -> str:
    return """
        SELECT
            sku,
            item_name,
            SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :yesterday THEN quantity ELSE 0 END) as qty_old,
            SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :today     THEN quantity ELSE 0 END) as qty_new,
            (SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :today     THEN quantity ELSE 0 END) -
             SUM(CASE WHEN SUBSTR(report_timestamp, 1, 10) = :yesterday THEN quantity ELSE 0 END)) as delta,

            -- FIX 1: уникальные дни до вчера (не строки) — точнее отражает «возраст» товара
            (SELECT COUNT(DISTINCT SUBSTR(s_hist.report_timestamp, 1, 10))
             FROM stocks s_hist
             WHERE s_hist.item_name = stocks.item_name
               AND SUBSTR(s_hist.report_timestamp, 1, 10) < :yesterday) AS history_count,

            -- FIX 2: переименование — тот же SKU, другое имя, ещё не обработанное
            --   ORDER BY report_timestamp DESC — самое свежее совпадение (не случайное)
            --   sku != '' — игнорируем пустые артикулы
            --   NOT EXISTS item_aliases — уже обработали это переименование
            --   NOT EXISTS coexist — оба имени никогда не были в продаже одновременно
            --     (если были вместе хоть раз → это два разных товара, не переименование)
            (SELECT s_sku.item_name
             FROM stocks s_sku
             WHERE s_sku.sku        = stocks.sku
               AND s_sku.sku       != ''
               AND s_sku.item_name != stocks.item_name
               AND NOT EXISTS (
                   SELECT 1 FROM item_aliases ia
                   WHERE ia.new_name = stocks.item_name
                     AND ia.old_name = s_sku.item_name
               )
               AND NOT EXISTS (
                   -- Кандидат «старого» имени никогда не появлялся в stocks
                   -- в тот же день что и «новое» имя → иначе это два разных товара
                   SELECT 1 FROM stocks c_old
                   WHERE c_old.item_name = s_sku.item_name
                     AND EXISTS (
                         SELECT 1 FROM stocks c_new
                         WHERE c_new.item_name = stocks.item_name
                           AND SUBSTR(c_new.report_timestamp, 1, 10)
                             = SUBSTR(c_old.report_timestamp, 1, 10)
                     )
               )
             ORDER BY s_sku.report_timestamp DESC
             LIMIT 1) AS old_name_alias,

            -- FIX 3: смена артикула — то же имя, другой SKU, ещё не обработанное
            --   ORDER BY report_timestamp DESC — самый свежий старый SKU
            --   фильтр мусорных SKU: '', '0', '-', 'null', 'н/а', 'нет'
            --   NOT EXISTS coexist — оба SKU для одного имени никогда не были в один день
            (SELECT s_name.sku
             FROM stocks s_name
             WHERE s_name.item_name = stocks.item_name
               AND s_name.sku      != stocks.sku
               AND LOWER(TRIM(s_name.sku)) NOT IN ('', '0', '-', 'null', 'нет', 'н/а', 'none', 'no')
               AND NOT EXISTS (
                   SELECT 1 FROM item_aliases ia
                   WHERE ia.new_name = stocks.item_name
               )
               AND NOT EXISTS (
                   -- Одно имя с ОБОИМИ артикулами в один день → дублирование данных,
                   -- а не реальная смена SKU
                   SELECT 1 FROM stocks c_old
                   WHERE c_old.item_name = stocks.item_name
                     AND c_old.sku       = s_name.sku
                     AND EXISTS (
                         SELECT 1 FROM stocks c_new
                         WHERE c_new.item_name = stocks.item_name
                           AND c_new.sku       = stocks.sku
                           AND SUBSTR(c_new.report_timestamp, 1, 10)
                             = SUBSTR(c_old.report_timestamp, 1, 10)
                     )
               )
             ORDER BY s_name.report_timestamp DESC
             LIMIT 1) AS old_sku_alias


        FROM stocks
        WHERE (SUBSTR(report_timestamp, 1, 10) = :today
               OR SUBSTR(report_timestamp, 1, 10) = :yesterday)
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