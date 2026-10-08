{% macro business_date() -%}
    {%- set value = var('business_date', none) -%}
    {%- if execute and value is none -%}
        {{ exceptions.raise_compiler_error("Missing Business Date: pass --vars '{business_date: YYYY-MM-DD}'") }}
    {%- endif -%}
    toDate('{{ value }}')
{%- endmacro %}

{# Pre-hook: a rerun replaces its Business Date's partition, even if it now produces no rows. #}
{% macro delete_business_date() -%}
    {%- if is_incremental() -%}
        alter table {{ this }} drop partition tuple({{ business_date() }})
    {%- endif -%}
{%- endmacro %}
