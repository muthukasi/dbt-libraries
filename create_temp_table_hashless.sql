{% macro create_temp_table_hashless (stgdatabase, stgschema, stgtable, inttable, multiplesrctblfl, materializationname) %}

  CREATE  temporary table {{inttable}}_TMP
  AS {{ sql }}

{% endmacro %}
