{% macro avg_field(field) %}
    AVG({{ field }})
{% endmacro %}