{# Gold tables: Delta files read by delta_scan on duckdb; Unity Catalog tables on databricks. #}
{% macro lake(table) -%}
  {%- if target.type == 'duckdb' -%}
    delta_scan('{{ env_var("BABEL_DATA", "../data") }}/lake/gold/{{ table }}')
  {%- else -%}
    {{ source('gold', table) }}
  {%- endif -%}
{%- endmacro %}
