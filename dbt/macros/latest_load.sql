{% macro latest_bronze_load(table) -%}
    select *
    from {{ source('bronze', table) }}
    where _business_date = {{ business_date() }}
      and _loaded_at = (
          select max(_loaded_at) from {{ source('bronze', 'shop_loads') }}
          where _business_date = {{ business_date() }}
      )
{%- endmacro %}

{% macro latest_catalog_load(table) -%}
    select *
    from {{ source('bronze', table) }}
    where _loaded_at = (select max(_loaded_at) from {{ source('bronze', 'catalog_loads') }})
{%- endmacro %}
