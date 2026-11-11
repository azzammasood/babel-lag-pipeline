{# A range test without a package dependency (dbt_utils needs a network install). #}
{% test value_in_range(model, column_name, min, max) %}
select * from {{ model }} where {{ column_name }} < {{ min }} or {{ column_name }} > {{ max }}
{% endtest %}
