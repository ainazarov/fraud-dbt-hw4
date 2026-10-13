{% macro customer_key(columns) -%}
    {%- set components = [] -%}
    {%- for column in columns -%}
        {%- do components.append("concat(toString(lengthUTF8(" ~ column ~ ")), ':', " ~ column ~ ")") -%}
    {%- endfor -%}
    {%- set encoded = "concat(" ~ components | join(", '|', ") ~ ")" -%}
    {{ dbt_utils.generate_surrogate_key([encoded]) }}
{%- endmacro %}
