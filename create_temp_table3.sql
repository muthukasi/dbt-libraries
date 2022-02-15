{% macro create_temp_table3 (inttable, keycolumn, valcolumnlist, sql, tgttblcpy) %}

{% if tgttblcpy == 'N' %}

    CREATE  temporary table {{ inttable.split(".")[2] }}_TMP2
    AS SELECT {{ hash_key_value(tablename=inttable,keycolumn=keycolumn) }} AS KEY_HASH_KEY,
    {{ valcolumnlist }} FROM  {{ inttable }}

{% else %}

    CREATE  temporary table {{ inttable }}_TMP3
    AS SELECT {{ hash_key_value(tablename=inttable,keycolumn=keycolumn) }} AS KEY_HASH_KEY,
    {{ valcolumnlist }} FROM  {{ inttable }}

{% endif %}

{% endmacro %}
