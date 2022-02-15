{% macro create_temp_table (stgdatabase, stgschema, stgtable, inttable, multiplesrctblfl,valcolumn,keycolumn ) %}

{% if multiplesrctblfl == 'N' %}

      CREATE  temporary table {{inttable}}_TMP
      AS SELECT {{ hash_dynamic(stgdatabase, stgschema, stgtable) }},
      A.* FROM {{stgdatabase}}.{{stgschema}}.{{stgtable}} A

{% else %}

    CREATE  temporary table {{inttable}}_TMP2
    AS SELECT {{ hash_key_value(tablename=inttable,valcolumn=valcolumn,keycolumn=keycolumn) }},
    A.* FROM {{stgdatabase}}.{{stgschema}}.{{stgtable}} A

{% endif %}

{% endmacro %}
