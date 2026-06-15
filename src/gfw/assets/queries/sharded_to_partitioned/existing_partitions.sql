SELECT partition_id
FROM `{{ project }}.{{ dataset }}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = '{{ table }}'
  AND partition_id != '__NULL__'
