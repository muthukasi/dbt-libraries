{% materialization scd_type_3_reusable, default %}

{% set src_db_nm = config.get('src_db_nm',default='none') %}
{% set src_sch_nm = config.get('src_sch_nm',default='none') %}
{% set src_tbl_nm = config.get('src_tbl_nm',default='none') %}
{% set tgt_db_nm = config.get('tgt_db_nm',default='none') %}
{% set tgt_sch_nm = config.get('tgt_sch_nm',default='none') %}
{% set tgt_tbl_nm = config.get('tgt_tbl_nm',default='none') %}
{% set key_cols = config.get('key_cols') %}
{% set odate = var('odate') %}
{% set skcolumn = config.get('sk_column') %}
{% set efectdate = var('update_date') %}
{% set hashkey = var('hash_key') %}
{% set tgttablerelattion = api.Relation.create(
       database = tgt_db_nm,
       schema = tgt_sch_nm,
       identifier = tgt_tbl_nm) -%}
{% set columnlist = adapter.get_columns_in_relation(tgttablerelattion) %}
{% set columns = columnlist | map(attribute="name") | join(',') %}
{% set value_cols_strategy = config.get('value_cols_strategy') %}
{% set materialization = config.get('materialized') %}
{% set dupchkfl = config.get('dupchkfl') %}
{% set historyversion = config.get('history_version_retained') %}
{% set valuecolumndict = config.get('current_history_value_column') %}

{% set keycolumnlist = key_cols.split(',') %}

{% for currentcolumn, historycolumnlist in valuecolumndict[0].items() %}

  {% set parsedvaluecolumnlist = historycolumnlist.split(',') %}

  {% if parsedvaluecolumnlist|length != historyversion %}

    {{ exceptions.raise_compiler_error("Invalid history version and history column provided in "~historycolumnlist~". Expected "~historyversion~" columns in the list "~historycolumnlist) }}

  {% endif %}

{% endfor %}

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
%}

{% set tgtcolumnlist = adapter.get_columns_in_relation(temptable) %}
{% set tgtcolumns = tgtcolumnlist | map(attribute="name") | join(',') %}

{% set col = adapter.get_missing_columns(tgttemptablerelation, tgttablerelattion) %}

{% if (col|length)  > 0 %}

  {{ audit_insert(invocation_id, odate, 'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SCHEMA MISMATCH BETWEEN SOURCE AND TARGET TABLE HAPPENED', 'F' ) }}

  {{ exceptions.raise_compiler_error("Column mismatch between Integration Table/query and Target Table. " ~col | map(attribute="name") | join(', ')~" doesnot exist in target table") }}

{% endif %}

{% set historycombinedcolumnlist =[] %}

{% for currentcolumn, historycolumnlist in valuecolumndict[0].items() %}

  {% set parsedvaluecolumnlist = historycolumnlist.split(',') %}

    {% for x in parsedvaluecolumnlist %}

        {{ historycombinedcolumnlist.append(x) }}

    {% endfor %}

{% endfor %}

{% set historycolumn = historycombinedcolumnlist | join(',') %}

{% set currentcombinedcolumnlist = [] %}

{% for currentcolumn, historycolumnlist in valuecolumndict[0].items() %}

  {% set parsedvaluecolumnlist = currentcolumn.split(',') %}

    {% for z in parsedvaluecolumnlist %}

        {{ currentcombinedcolumnlist.append(z) }}

    {% endfor %}

{% endfor %}

{% for historycolumn in historycombinedcolumnlist %}

  {% if historycolumn not in columns %}

    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'COLUMN '~historycolumn~' IS SUPPLIED IN CONFIG. BUT NOT AVAILABLE IN TARGET TABLE', 'F' ) }}

    {{ exceptions.raise_compiler_error("Column " ~historycolumn~" doesnot exist in target table but supplied in model config file") }}

  {% endif %}

{% endfor %}

{% for currentcolumn in currentcombinedcolumnlist %}

  {% if currentcolumn not in columns %}

    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'COLUMN '~currentcolumn~' IS SUPPLIED IN CONFIG. BUT NOT AVAILABLE IN TARGET TABLE', 'F' ) }}

    {{ exceptions.raise_compiler_error("Column " ~currentcolumn~" doesnot exist in target table but supplied in model config file") }}

  {% endif %}

{% endfor %}

{% for keycolumn in keycolumnlist %}

  {% if keycolumn not in columns %}

    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'COLUMN '~keycolumn~' IS SUPPLIED IN CONFIG. BUT NOT AVAILABLE IN TARGET TABLE', 'F' ) }}

    {{ exceptions.raise_compiler_error("Column " ~keycolumn~" doesnot exist in target table but supplied in model config file") }}

  {% endif %}

{% endfor %}

{% set y = historycombinedcolumnlist %}

{% for x in currentcombinedcolumnlist %}

  {{ y.append(x) }}

{% endfor %}

{% set combinedcolumn = y | join(',') %}

{% set currentcolumn = currentcombinedcolumnlist | join(',') %}

{% set currentcolumnlistind = [] %}

{% for x in currentcombinedcolumnlist %}

  {{ currentcolumnlistind.append(x + ",'N' AS " + x + "_IND") }}

{% endfor %}

{% set currentcolumnind = currentcolumnlistind | join(',') %}

{% set qualifiedtgttblnm = tgt_db_nm~"."~tgt_sch_nm~"."~tgt_tbl_nm %}

{% set qualifiedinttblnm = src_db_nm~"."~src_sch_nm~"."~src_tbl_nm %}

{% call statement() %}

  {{ create_temp_table3 (inttable=qualifiedtgttblnm, keycolumn=key_cols, valcolumnlist=currentcolumn, tgttblcpy='N') }}

{% endcall %}

{% call statement() %}

  {{ create_temp_table3 (inttable=tgt_tbl_nm~'_TMP', keycolumn=key_cols, valcolumnlist=currentcolumnind, tgttblcpy='Y') }}

{% endcall %}

{% call statement() %}

  UPDATE {{ tgt_tbl_nm }}_TMP_TMP3 A SET
  {% for x in currentcombinedcolumnlist %}
    {{ x }}_IND = CASE WHEN A.{{ x }}<>B.{{ x }} THEN 'Y' ELSE 'N' END {{ "," if not loop.last }}
  {% endfor %}
  FROM {{ qualifiedtgttblnm.split(".")[2] }}_TMP2 B
  WHERE A.KEY_HASH_KEY=B.KEY_HASH_KEY

{% endcall %}

{% set sourcetable = tgt_tbl_nm~"_TMP_TMP3" %}

{% call statement('update',fetch_result=true) %}

  {{ scd_type3_update (sourcetable, qualifiedtgttblnm, key_cols, valuecolumndict, efectdate, odate, hashkey ) }}

{% endcall %}

{% set tempupdcounts = load_result('update')['data'] %}

{% set update_counts = tempupdcounts[0][0] %}

{% call statement('main',fetch_result=true) -%}

  INSERT INTO {{tgt_db_nm}}.{{tgt_sch_nm}}.{{tgt_tbl_nm}}
  ( {{ tgtcolumns }} )
  {{ sql }}
  {{ " WHERE NOT EXISTS (SELECT 1 FROM "}}
  {{tgt_db_nm}}.{{tgt_sch_nm}}.{{tgt_tbl_nm}}
  {{ " WHERE "}}{{ hash_key_value(tablename=tgt_db_nm~"."~tgt_sch_nm~"."~tgt_tbl_nm, keycolumn=key_cols) }}
  {{ "=" }}{{ hash_key_value(tablename=src_db_nm~"."~src_sch_nm~"."~src_tbl_nm, keycolumn=key_cols) }}
  {{ ")" }}
{% endcall %}

{% set tempinscounts = load_result('main')['data'] %}

{% set insert_counts = tempinscounts[0][0] %}

/*-------------------PERFORM HASH CHECK FROM HERE -----------------------------------------------*/



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

{{ audit_insert(invocation_id, odate, 'SUCCESS', modelexestrttime, current_timestamp(), insert_counts, update_counts, 0, 'ALL CONTROLS PASSED', 'P' ) }}

{% do adapter.commit() %}

{{ return({'relations': [tgttablerelattion]}) }}

{% endmaterialization %}
