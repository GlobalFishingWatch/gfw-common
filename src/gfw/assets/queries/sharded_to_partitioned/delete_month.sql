DELETE FROM `{{ target }}`
WHERE DATE_TRUNC(DATE(timestamp), MONTH) = '{{ year }}-{{ month }}-01'
