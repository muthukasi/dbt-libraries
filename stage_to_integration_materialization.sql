{% materialization stage_to_integration, default %}

/*-----Get all parameter values from Config file ------*/

{% set stgtablelist = config.get('stagingtable') %}
{% set intermediatetable = config.get('integrationtable') %}
{% set keyfields = config.get('keycolumns') %}
{% set stgdatabase = config.get('stgdb') %}
{% set stgschema = config.get('stgschema') %}
{% set dupchkfl = config.get('dupchkfl') %}
{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone(var("time_zone"))) -%}
{% set odate = config.get('odate') %}
{% set multiplesrctblfl = config.get('multiplesrctblfl') %}
{% set materializationname = config.get('materialized') %}
{% set temptable = intermediatetable~"_TMP" %}
{% set temptable2 = intermediatetable~"_TMP_TMP2" %}
{% set hash_key=var('hash_key') %}
{% set etl_recorded_ts=var('etl_recorded_ts') %}

{% if dupchkfl == 'Y' %}

  {% if keyfields is none %}

    {{ exceptions.raise_compiler_error("Duplicate Check is enabled. But keycolumns are not defined in config.") }}

  {% endif %}

{% endif %}

{% if multiplesrctblfl == 'Y' %}

  {% if stgtablelist is string %}

      {{ exceptions.raise_compiler_error("Multiple Source Stage Flag is enabled. But Staging table is not provided inside square brackets.") }}

  {% endif %}

{% endif %}

{% if multiplesrctblfl == 'Y' %}

  {% for stagingtable in stgtablelist %}

    {% set stagingexist = adapter.get_relation(
    database = stgdatabase,
    schema = stgschema,
    identifier = stagingtable)
    %}

/*-----Check if staging table exists----------------*/

    {% if  stagingexist is none %}

      {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F' ) }}

      {{ exceptions.raise_compiler_error("Staging Table " ~stagingtable~" doesnot exist") }}

    {% endif %}

  {% endfor %}

{% else %}

  {% set stagingexist = adapter.get_relation(
  database = stgdatabase,
  schema = stgschema,
  identifier = stgtablelist)
  %}

  {% if stagingexist is none %}

    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F' ) }}

    {{ exceptions.raise_compiler_error("Staging Table " ~stgtablelist~" doesnot exist") }}

  {% endif %}

{% endif %}

{% set intermediateexist = adapter.get_relation(
       database = stgdatabase,
       schema = stgschema,
       identifier = intermediatetable)
  %}

/*-------Check if intermediate table exists---------*/

{% if  intermediateexist is none %}

  {{ audit_insert(invocation_id, odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'INTERMEDIATE TABLE DOESNOT EXIST', 'F' ) }}

  {{ exceptions.raise_compiler_error("Integration/Target Staging Table " ~intermediatetable~" doesnot exist") }}

{% endif %}

/*-------------Create Temp Table---------------------*/

{% call statement() %}

  {{ create_temp_table_hashless (inttable=intermediatetable,materializationname=materializationname) }}

{% endcall %}


{% set intermediatetablerelattion = api.Relation.create(
       database = stgdatabase,
       schema = stgschema,
       identifier = intermediatetable) -%}
%}


{% set stagingtablerelattion = api.Relation.create(
       identifier = temptable) -%}
%}

/*---------Check if there is mismatch between staging table and intermediate table without Hash Key-----*/

{% set col = adapter.get_missing_columns(intermediatetablerelattion, stagingtablerelattion) %}

{% if (col|length)  != 0 %}

  {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SCHEMA MISMATCH BETWEEN STG TABLE AND INT TABLE HAPPENED', 'F' ) }}

  {{ exceptions.raise_compiler_error("Column mismatch between Staging and Intermediate table. " ~col | map(attribute="name") | join(', ')~" doesnot exist in intermediate table") }}

{% endif %}

{% set columnlist = adapter.get_columns_in_relation(stagingtablerelattion) %}

{% set columns = columnlist | map(attribute="name") | join(', ') %}

{% set columnnamelist = columnlist | map(attribute="name") %}

/*-------GET VALUE COLUMNS BY IGNORING HASH_KEY AND ETL_RECORDED_TS----*/

{% set y = [] %}

{% for x in columnnamelist %}

  {% if x != hash_key and x != etl_recorded_ts %}

    {{ y.append(x) }}

  {% endif %}

{% endfor %}

{% set valcolumn = y | join(',') %}

/*-----CREATE TEMPTABLE 2 WITH HASH KEY AND TIMESTAMP COLUMN--------*/

{% call statement() %}

  {{ create_temp_table2 (inttable=temptable, materializationname=materializationname, valcolumn=valcolumn) }}

{% endcall %}

/*-------Purge if any data exists in intermediate table------------*/

{% call statement() %}

  {{ purge_data (stgdatabase, stgschema, intermediatetable) }}

{% endcall %}

/*---------Execute the actual insert statement. STG -> INT Table--------*/

{%- call statement('main',fetch_result=true) -%}

  INSERT INTO {{stgdatabase}}.{{stgschema}}.{{intermediatetable}}
  ( {{ columns }} )
  {{ sql }}

{% endcall %}

/*---{{ adapter.commit() }}----*/

/*------Check hash value between intermediate table and Temp table-----*/


{% set hash_check %}

  {{ check_hash (materializationname=materializationname, stgdatabase=stgdatabase, stgschema=stgschema, sourcetable=intermediatetable, targettable=temptable2) }}

{% endset %}

{% set results = run_query(hash_check) %}

{% if execute %}

  {% set hash_check_result = results.columns[0].values() %}

{% else %}

  {% set hash_check_result = [] %}

{% endif %}

{% if hash_check_result[0] != 0 %}

  {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'HASH VALUE CHECK FAILED', 'F' ) }}

  {{ exceptions.raise_compiler_error("Hash value is not matching while loading. Try rerunning the load again") }}

{% endif %}

/*------Check if duplicate values exsists for passed key values------*/

{% if dupchkfl == 'Y' %}

  {% set duplicate_check %}

    {{ check_duplicates (materializationname, stgdatabase, stgschema, keyfields, 'none') }}

  {% endset %}

  {% set dupsresults = run_query(duplicate_check) %}

  {% if execute %}

    {% set dups_check_result = dupsresults.columns[0].values() %}

  {% else %}

    {% set dups_check_result = [] %}

  {% endif %}

  {% if dups_check_result|length %}

    {{ audit_insert(invocation_id, odate, 'ABORTED',modelexestrttime, current_timestamp(), 0, 0, 0, 'DUPLICATE VALUE EXISTS IN SOURCE', 'F' ) }}

    {{ exceptions.raise_compiler_error("Duplicate values exists in source for the key columns "~keyfields~". Please check with source") }}

  {% endif %}

{% endif %}

/*--------Get Counts-------------------------------------------------*/

{% set insertcounts = load_result('main')['data'] %}

{% set insert_counts = insertcounts[0][0] %}

  {{ audit_insert(invocation_id, odate, 'SUCCESS', modelexestrttime, current_timestamp(), insert_counts, 0, 0, 'ALL CONTROLS PASSED', 'P' ) }}

{% do adapter.commit() %}

{{ log("All the defined controls passed. Data got loaded successfully from "~stgtablelist~" to "~intermediatetable~".", info=True ) }}

{{ return({'relations': [intermediatetablerelattion]}) }}

{% endmaterialization %}
