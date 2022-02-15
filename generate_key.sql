{% macro generate_key(tablename,skcolumn) %}

  (SELECT COALESCE(MAX({{ skcolumn }}),0) FROM {{tablename}} )+ROW_NUMBER() OVER(ORDER BY NULL) AS {{ skcolumn }}

{% endmacro %}
