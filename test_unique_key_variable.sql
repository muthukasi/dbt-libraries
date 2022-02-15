{% macro test_unique_key_variable(source, sourcefield, model, column_name) %}

select count(*) from {{ model }} A where not exists (
  select 1 from {{ source }} B where A.{{ column_name }}=B.{{ sourcefield }}
)

{% endmacro %}
