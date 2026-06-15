SELECT DISTINCT column_name
FROM `{{ fq_dataset }}.INFORMATION_SCHEMA.COLUMNS`
WHERE REGEXP_CONTAINS(table_name, r'^{{ table_id }}_[0-9]{8}$')
