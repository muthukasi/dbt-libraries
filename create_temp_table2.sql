{% macro create_temp_table2 (inttable, valcolumn, keycolumn, materializationname) %}

{% if materializationname == 'scd_type_2_reusable' %}

    CREATE  temporary table {{inttable}}_TMP2
    AS SELECT {{ hash_key_value(tablename=inttable, valcolumn=valcolumn, keycolumn=keycolumn) }},
    A.* FROM  {{ inttable }} A

{% elif materializationname == 'stage_to_integration' %}

    CREATE  temporary table {{inttable}}_TMP2
    AS SELECT {{ hash_key_value(tablename=inttable, valcolumn=valcolumn, materializationname=materializationname) }},
    CAST({{ tz_timestamp(current_timestamp()) }} AS TIMESTAMP) AS ETL_RECORDED_TS,
    {{ valcolumn }} FROM  {{ inttable }}

{% elif materializationname == 'scd_type_3_reusable' %}

    CREATE  temporary table {{ inttable.split(".")[2] }}_TMP2
    AS SELECT {{ hash_key_value(tablename=inttable, valcolumn=valcolumn, keycolumn=keycolumn) }},
    A.* FROM  {{ inttable }} A

{% endif %}

{% endmacro %}
