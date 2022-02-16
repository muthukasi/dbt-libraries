{% materialization scd_type_2_reusable, default %}

{% set src_db_nm = config.get('src_db_nm',default='none') %}
{% set src_sch_nm = config.get('src_sch_nm',default='none') %}
{% set src_tbl_nm = config.get('src_tbl_nm',default='none') %}
{% set tgt_db_nm = config.get('tgt_db_nm',default='none') %}
{% set tgt_sch_nm = config.get('tgt_sch_nm',default='none') %}
{% set tgt_tbl_nm = config.get('tgt_tbl_nm',default='none') %}
{% set key_cols = config.get('key_cols') %}
{% set odate = config.get('odate') %}
{% set skcolumn = config.get('sk_column') %}
{% set activeind = var('active_ind') %}
{% set validfromts = var('start_ts') %}
{% set validtots = var('end_ts') %}
{% set hashkey = var('hash_key') %}
{% set tgttablerelattion = api.Relation.create(
       database = tgt_db_nm,
       schema = tgt_sch_nm,
       identifier = tgt_tbl_nm) -%}
%}
{% set columnlist = adapter.get_columns_in_relation(tgttablerelattion) %}
{% set columns = columnlist | map(attribute="name") | join(',') %}
{% set value_cols_strategy = config.get('value_cols_strategy') %}
{% set loadstrtgy = config.get('load_strategy', default='DELTA').upper() %}
{% set materialization = config.get('materialized') %}
{% set dupchkfl = config.get('dupchkfl') %}
{% set forcedelete = config.get('force_delete', default='N') %}
{% set delete_counts = 0 %}


{% if loadstrtgy.upper() != "DELTA" and loadstrtgy.upper() != "FULL" %}

  {{ exceptions.raise_compiler_error("Invalid value passed for load_strategy in model config. Allowed values are 'DELTA', 'FULL'.") }}

{% endif %}

{% if value_cols_strategy.upper() != "ALL" and value_cols_strategy.upper() != "EXCEPT" and value_cols_strategy.upper() != "ONLY" %}

  {{ exceptions.raise_compiler_error("Invalid value passed for value_cols_strategy in model config. Allowed values are 'ALL', 'EXCEPT', 'ONLY'") }}

{% endif %}

{% if value_cols_strategy.upper() == "ONLY" %}

  {% set val_cols = config.get('value_cols', default='none') %}

  {% if val_cols|length and val_cols != 'none' %}

  {% else %}

    {{ exceptions.raise_compiler_error("Invalid value passed for value_cols in model config. value_cols_strategy is passed as ONLY but value_cols is empty") }}

  {% endif %}

{% elif value_cols_strategy.upper() == "ALL" %}

  {% set audit_column_list_tmp = skcolumn~","~activeind~","~validfromts~","~validtots~","~hashkey %}

  {% set audit_column_list = audit_column_list_tmp.split(',') %}

  {% set columnset = columns.split(',') %}

  {% set y = [] %}

  {% for x in columnset %}

    {% if x not in audit_column_list %}

      {{ y.append(x) }}

    {% endif %}

  {% endfor %}

{% set value_cols = y | join(',') %}

{% elif value_cols_strategy.upper() == "EXCEPT" %}

  {% set audit_column_list_tmp = skcolumn~","~activeind~","~validfromts~","~validtots~","~hashkey~","~config.get('value_cols', default='none') %}

  {% set audit_column_list = audit_column_list_tmp.split(',') %}

    {% set columnset = columns.split(',') %}

    {% set y = [] %}

    {% for x in columnset %}

      {% if x not in audit_column_list %}

        {{ y.append(x) }}

      {% endif %}

    {% endfor %}

  {% set value_cols = y | join(',') %}

{% endif %}

{% if key_cols is none %}

  {{ exceptions.raise_compiler_error("Key Columns cannot be empty for SCD Type 2. Please enable key_cols in model config.") }}

{% endif %}

{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone(var("time_zone"))) -%}

{% set temptable = tgt_tbl_nm~"_TMP" %}

{% set sourceexists = adapter.get_relation(
                      database = src_db_nm,
                      schema = src_sch_nm,
                      identifier = src_tbl_nm)
%}

{% if sourceexists is none %}

  {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'INTEGRATION TABLE DOESNOT EXIST', 'F' ) }}

  {{ exceptions.raise_compiler_error("Integration Table " ~src_tbl_nm~" doesnot exist.") }}

{% endif %}

{% set targetexists = adapter.get_relation(
                      database = tgt_db_nm,
                      schema = tgt_sch_nm,
                      identifier = tgt_tbl_nm)
%}

{% if targetexists is none %}

  {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'TARGET TABLE DOESNOT EXIST', 'F' ) }}

  {{ exceptions.raise_compiler_error("Target Table " ~tgt_tbl_nm~" doesnot exist.") }}

{% endif %}

{% call statement() %}

  {{ create_temp_table_hashless (src_db_nm, src_sch_nm, src_tbl_nm, tgt_tbl_nm, 'N') }}

{% endcall %}

{% set tgttemptablerelation = api.Relation.create(
       identifier = temptable) -%}

{% set col = adapter.get_missing_columns(tgttemptablerelation, tgttablerelattion) %}

{% if (col|length)  > 0 %}

  {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SCHEMA MISMATCH BETWEEN SOURCE AND TARGET TABLE HAPPENED', 'F' ) }}

  {{ exceptions.raise_compiler_error("Column mismatch between Integration Table/query and Target Table. " ~col | map(attribute="name") | join(', ')~" doesnot exist in target table") }}

{% endif %}

{% call statement() %}

  {{ create_temp_table2 (inttable=temptable, valcolumn=value_cols, keycolumn=key_cols, materializationname=materialization) }}

{% endcall %}

{% set temptablename = temptable~"_TMP2" %}

{% set targettable = tgt_db_nm~"."~tgt_sch_nm~"."~tgt_tbl_nm %}

{% if loadstrtgy.upper() == "FULL" %}

  {% set IntegrationTableCount =  tbl_count_chck(src_tbl_nm, src_db_nm, src_sch_nm) %}

    {% if IntegrationTableCount == 0 and forcedelete == 'N' %}

      {{ exceptions.raise_compiler_error("Caution!!! loadstrtgy is set as FULL. But Integration table is empty. This may end date all the records in Target Table : "~tgt_tbl_nm~". If you are sure about this add the flag force_delete='Y' in config file and rerun the model.") }}

    {% endif %}

    {% call statement('delete',fetch_result=true ) %}

      {{ scd_type2_delete(temptablename,targettable,key_cols,value_cols) }}

    {% endcall %}

    {% set tempdelcounts = load_result('delete')['data'] %}

    {% set delete_counts = tempdelcounts[0][0] %}

{% endif %}

{% call statement('update',fetch_result=true) %}

  {{ scd_type2_update(temptablename,targettable,key_cols,value_cols) }}

{% endcall %}

{% set tempupdcounts = load_result('update')['data'] %}

{% set update_counts = tempupdcounts[0][0] %}

{% call statement('main',fetch_result=true) -%}

  INSERT INTO {{tgt_db_nm}}.{{tgt_sch_nm}}.{{tgt_tbl_nm}}
  ( {{ columns }} )
  {{ sql }}
  {{ " WHERE NOT EXISTS (SELECT 1 FROM "}}
  {{tgt_db_nm}}.{{tgt_sch_nm}}.{{tgt_tbl_nm}}
  {{ " WHERE "}}{{ hash_key_value(tablename=tgt_db_nm~"."~tgt_sch_nm~"."~tgt_tbl_nm, keycolumn=key_cols) }}
  {{ "=" }}{{ hash_key_value(tablename=src_db_nm~"."~src_sch_nm~"."~src_tbl_nm, keycolumn=key_cols) }}
  {{ " AND " }} {{tgt_db_nm}}.{{tgt_sch_nm}}.{{tgt_tbl_nm}}{{".ACTIVE_IND='Y') " }}

{% endcall %}

{% set tempinscounts = load_result('main')['data'] %}

{% set insert_counts = tempinscounts[0][0] %}

/*-------------------PERFORM HASH CHECK FROM HERE -----------------------------------------------*/

{% set hash_check %}

  {{ check_hash (materialization, src_db_nm, src_sch_nm, src_tbl_nm, tgt_db_nm, tgt_sch_nm, tgt_tbl_nm, loadstrtgy, activeind) }}

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

/*-------------------PERFORM DUPLICATE CHECK FROM HERE -----------------------------------------------*/

{% if dupchkfl == 'Y' %}

  {% set duplicate_check %}

    {{ check_duplicates (materialization, tgt_db_nm, tgt_sch_nm, key_cols, activeind) }}

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

{{ audit_insert(invocation_id, odate, 'SUCCESS', modelexestrttime, current_timestamp(), insert_counts, update_counts, delete_counts, 'ALL CONTROLS PASSED', 'P' ) }}

{% do adapter.commit() %}

{{ return({'relations': [tgttablerelattion]}) }}

{% endmaterialization %}
