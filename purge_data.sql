{% macro purge_data(stgdatabase, stgschema, tablename, whereclause) %}

  DELETE FROM {{ stgdatabase }}.{{ stgschema }}.{{ tablename }}

{% endmacro %}
