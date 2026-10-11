{% macro multiply(x, y, precision) %}
    ROUND({{ x }} * {{ y }}, {{ precision }})
{% endmacro %}