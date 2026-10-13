{% macro amount_bucket(expression) -%}
    case
        when {{ expression }} < 100 then 'SMALL'
        when {{ expression }} < 500 then 'MEDIUM'
        else 'LARGE'
    end
{%- endmacro %}
