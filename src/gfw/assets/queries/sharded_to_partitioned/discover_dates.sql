SELECT SUBSTR(table_name, {{ substr_start }}) AS date
FROM `{{ fq_dataset }}.INFORMATION_SCHEMA.TABLES`
WHERE REGEXP_CONTAINS(table_name, r'^{{ table_id }}_[0-9]{8}$')
ORDER BY date
