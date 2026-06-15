{% for source in sources %}
{% if not loop.first %}
UNION ALL
{% endif %}
SELECT
    {% for col in source.cols %}
    {{ col }}{{ "," if not loop.last }}
    {% endfor %}
FROM `{{ source.fqn }}_{{ source.date }}`
{% endfor %}
