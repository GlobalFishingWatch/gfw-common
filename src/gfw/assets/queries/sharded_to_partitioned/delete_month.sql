DELETE FROM `{{ target }}`
WHERE DATE_TRUNC(DATE({{ partition_field }}), MONTH) = '{{ year }}-{{ month }}-01'
