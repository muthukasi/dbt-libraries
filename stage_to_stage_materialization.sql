{% materialization stage_to_stage, default %}

/*-----Get all parameter values from Config file ------*/

{% set integrationtable = config.get('targettable') %}
{% set keyfields = config.get('keycolumns') %}
{% set stgdatabase = config.get('stgdb') %}
{% set stgschema = config.get('stgschema') %}
{% set dupchkfl = config.get('dupchkfl') %}
{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone(var("time_zone"))) -%}
{% set odate = config.get('odate') %}
{% set stagingtable = config.get('staging') %}
{% set stgschema = config.get('stgschema') %}
{% set stgtablelist = config.get('stagingtable') %}
{% set multiplesrctblfl = config.get('multiplesrctblfl') %}
{% set temptable = integrationtable~"_TMP" %}
{% set materializationname = config.get('materialized') %}

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

      {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F') }}

      {{ exceptions.raise_compiler_error("Staging Table " ~stagingtable~" doesnot exist") }}

    {% endif %}

  {% endfor %}

{% else %}

  {% set stagingtable = stgtablelist %}

  {% set stagingexist = adapter.get_relation(
         database = stgdatabase,
         schema = stgschema,
         identifier = stagingtable)
  %}

/*-----Check if staging table exists----------------*/

  {% if  stagingexist is none %}

    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F') }}

    {{ exceptions.raise_compiler_error("Staging Table " ~stagingtable~" doesnot exist") }}

  {% endif %}

{% endif %}

{% set integrationexist = adapter.get_relation(
       database = stgdatabase,
       schema = stgschema,
       identifier = integrationtable)
  %}

/*-------Check if integration table exists---------*/

{% if  integrationexist is none %}

  {{ audit_insert(invocation_id, odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'INTEGRATION TABLE DOESNOT EXIST', 'F') }}

  {{ exceptions.raise_compiler_error("integration Table " ~integrationtable~" doesnot exist") }}

{% endif %}

{% set integrationtablerelation = api.Relation.create(
       database = stgdatabase,
       schema = stgschema,
       identifier = integrationtable) -%}
%}

{% call statement() %}

  {{ create_temp_table_hashless (stgdatabase, stgschema, stagingtable, integrationtable, multiplesrctblfl) }}

{% endcall %}

{% set stagingtablerelation = api.Relation.create(
       identifier = temptable) -%}
%}

/*---------Check if there is mismatch between staging table and integration table without Hash Key-----*/

{% set col = adapter.get_missing_columns(integrationtablerelation, stagingtablerelation) %}

{% if (col|length)  > 0 %}

  {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SCHEMA MISMATCH BETWEEN SOURCE AND TARGET TABLE HAPPENED', 'F' ) }}

  {{ exceptions.raise_compiler_error("Column mismatch between Staging and Target table. " ~col | map(attribute="name") | join(', ')~" doesnot exist in target table") }}

{% endif %}

{% set columnlist = adapter.get_columns_in_relation(integrationtablerelation) %}

{% set columns = columnlist | map(attribute="name") | join(', ') %}

/*-------Purge if any data exists in integration table------------*/

{% call statement() %}

  {{ purge_data (stgdatabase, stgschema, integrationtable) }}

{% endcall %}

/*---------Execute the actual insert statement. STG -> INT Table--------*/

{% call statement('main',fetch_result=true) -%}

  INSERT INTO {{stgdatabase}}.{{stgschema}}.{{integrationtable}}
  ( {{ columns }} )
  {{ sql }}

{% endcall %}

/*------Check hash value between integration table and Temp table-----*/

{% set hash_check %}

  {{ check_hash (materializationname, stgdatabase, stgschema, temptable, integrationtable) }}

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

    {{ check_duplicates (stgdatabase, stgschema, keyfields) }}

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

{{ return({'relations': [integrationtablerelation]}) }}

{% endmaterialization %}
