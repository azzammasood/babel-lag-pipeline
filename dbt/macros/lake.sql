{# Gold tables: Delta files read by delta_scan on duckdb; Unity Catalog tables on databricks,
   where the per-language gold comes from the backfill (`_all`) once it exists (ADR-013). #}
{% macro lake(table) -%}
  {%- if target.type == 'duckdb' -%}
    delta_scan('{{ env_var("BABEL_DATA", "../data") }}/lake/gold/{{ table }}')
  {%- else -%}
    {%- if var('reference_set', 'slice20') == 'all'
          and table in ['gold_language_lag', 'gold_language_demand'] -%}
      {{ source('gold', table ~ '_all') }}
    {%- else -%}
      {{ source('gold', table) }}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
