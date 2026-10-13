{% macro clickhouse__day_of_week(date, isoweek=true) -%}
    {% if isoweek -%}
        toDayOfWeek({{ date }}, 0)
    {%- else -%}
        toDayOfWeek({{ date }}, 3)
    {%- endif %}
{%- endmacro %}
