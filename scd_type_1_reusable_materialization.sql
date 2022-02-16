{% materialization scd_type_1_reusable, default %}
{% set target_table_name = model['alias'] %}
{% set src_dtl = config.get('src_dtl',default='none') %}
{% set trgt_dtl = config.get('trgt_dtl',default='none') %}
{% set full_refresh_flag = config.get('full_refresh_flag',default='N') %}
{% set snapshot_refresh_flg=config.get('snapshot_refresh_flg', default='Y') %}
{% set key_cols=config.get('key_cols', default='none' )%}
{% set date_col = config.get('date_col_name',default='none') %}
{% set date_value = config.get('odate',default='none') %} /* added for delete and insert */




--{% set src_sch_nm = config.get('src_schema_name',default='none') %}
--{% set src_db_nm = config.get('src_db_name',default='none') %}
--{% set tgt_sch_nm = config.get('target_schema_name',default='none') %}
--{% set tgt_db_nm = config.get('target_db_name',default='none') %}
--{% set unique_key = config.get('unique_key',default='none') %}

--Below params are used for AUDIT table entries
{% set odate = config.get('odate') %}
{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone("America/Chicago")) -%}




{% set source_details = (source( src_dtl  ,'dual'))   %}
{% set src_db_nm= source_details['database'] %}
{% set src_sch_nm= source_details['schema'] %}


{% set target_details = (source( trgt_dtl ,'dual'))  %}
{% set tgt_db_nm= target_details['database'] %}
{% set tgt_sch_nm= target_details['schema'] %}



{% set validation_flag = 0 %}
{{ log ("#####################################################################"~full_refresh_flag,info=true )}}
{{ log ("######################INPUT PARAMETERS VALIDATION START##############"~full_refresh_flag,info=true )}}
{{ log ("#####################################################################"~full_refresh_flag,info=true )}}



{% set date = modules.datetime.date %}
{% set is_date = date.fromisoformat(odate)   %}


{{ log("is_date: " ~ is_date , info = true ) }}

{% if is_date is none %}
  {{ log("VALIDATION ERROR for odate ...value passed is not a date", info= true) }}
{% else %}
  {{ log("Odate value passed is a date type:" ~ odate , info= true) }}

{% endif %}

--{% if  odate | as_text %}
  --{{ log("odate value passed is date") }}
--{% endif %}
--######################INPUT PARAMETERS VALIDATION####################


-- Validation for full_refresh_flag
{% if full_refresh_flag =='Y' or full_refresh_flag=='N' %}
  {{ log ("Valid full_refresh_flag flag is passed: "~full_refresh_flag,info=true )}}
{% else %}
  {{ raise_validation_error("Invalid full_refresh_flag flag is passed: "+full_refresh_flag )}}
  {% set validation_flag = validation_flag+1 %}
{% endif %}

-- Validation for snapshot_refresh_flg
{% if snapshot_refresh_flg =='Y' or snapshot_refresh_flg=='N' %}
  {{ log ("Valid snapshot_refresh_flg flag is passed: "~snapshot_refresh_flg,info=true )}}
{% else %}
  {{ raise_validation_error("Invalid snapshot_refresh_flg flag is passed: " +snapshot_refresh_flg )}}
  {% set validation_flag = validation_flag+1 %}
{% endif %}


{% set tmp_identifier = target_table_name + '_tmp' %}
{% set int_tbl_identifier = target_table_name + '_int' %}



---Check availabilty of target table and key column(s) in target database

{% set trgt_tbl_availability %}
      select count(*) from {{tgt_db_nm}}."INFORMATION_SCHEMA"."TABLES" where upper(table_name)=UPPER('{{target_table_name}}')
      and table_schema='{{tgt_sch_nm}}'
{%- endset -%}
{% set results_tgt = run_generic_sql(trgt_tbl_availability ,tgt_db_nm,tgt_sch_nm,'Table availabilty check in target database','Y') %}


{% if results_tgt is none %}
  {{ raise_validation_error("Target DB "  + tgt_db_nm + " unavailable" ) }}
  {% if is_date is none %}
    {{ log("  No Audit entry made for Target table "  ~ target_table_name ~ " as ODATE value is not a date", info=true) }}
  {% else %}
    {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'TARGET TABLE DOESNOT EXIST', 'F' ) }}
    {% set validation_flag = validation_flag+1 %}
  {% endif %}

{% else %}
  {% if results_tgt == 1 %}
    {{ log("  -Target table "  ~ target_table_name ~ " is available in the database.schema " ~  tgt_db_nm ~ "." ~ tgt_sch_nm, info=true) }}
    --Check if key columns exist in target
    {% set chk_key_exist_in_target= chck_col_exists(target_table_name,tgt_db_nm, tgt_sch_nm,key_cols)  %}

    {% if chk_key_exist_in_target> 0 %}
      {{ raise_validation_error("Above mentioned columns do not exist in tgt_db_nm="+ tgt_db_nm +" and tgt_sch_nm="+ tgt_sch_nm) }}
      {% set validation_flag = validation_flag+1 %}
    {% else %}
      {{log("Key columns are present in the target table") }}
    {% endif %}


    {% if is_date is none %}
      {{ log("  No Audit entry made for Target table "  ~ target_table_name ~ " as ODATE value is not a date", info=true) }}
    {% else %}

      {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), 0, 0, 0, 'TARGET TABLE EXISTS', 'P' ) }}

    {% endif %}
  {% else %}
    {{ raise_validation_error( "Target table "  + target_table_name + " is not available in the database.schema " +  tgt_db_nm + "." + tgt_sch_nm) }}
    {% set validation_flag = validation_flag+1 %}
  {% endif %}
{% endif %}





--Check for intermediate/source table

  {% set chck_int_table %}
        select count(*) from {{src_db_nm}}."INFORMATION_SCHEMA"."TABLES" where UPPER(table_name)=UPPER('{{int_tbl_identifier}}')
        and table_schema='{{src_sch_nm}}'
  {%- endset -%}

  {% set results_src = run_generic_sql(chck_int_table ,src_db_nm,src_sch_nm,'Source table availabilty in source database','Y') %}
  {% if results_src is none %}
    {{ raise_validation_error("Source DB "  + src_db_nm + " unavailable" ) }}
    {% if is_date is none %}
      {{ log("  No Audit entry made for Source table "  ~ int_tbl_identifier ~ " as ODATE value is not a date", info=true) }}
    {% else %}
      {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'Source TABLE DOESNOT EXIST', 'F' ) }}
      {% set validation_flag = validation_flag+1 %}
    {% endif %}

  {% else %}
    {% if results_src == 1 %}
      {{ log("  -Source table "  ~ int_tbl_identifier ~ " is available in the database.schema " ~  src_db_nm ~ "." ~ src_sch_nm, info=true) }}
      --Check if key columns exist in Source
      {% set chk_key_exist_in_source= chck_col_exists(int_tbl_identifier,src_db_nm, src_sch_nm,key_cols)  %}

      {% if chk_key_exist_in_source> 0 %}
        {{ raise_validation_error("Above mentioned columns do not exist in src_db_nm="+ src_db_nm +" and src_sch_nm="+ src_sch_nm) }}
        {% set validation_flag = validation_flag+1 %}
      {% else %}
        {{log("Key columns are present in the Source table") }}
      {% endif %}


      {% if is_date is none %}
        {{ log("  No Audit entry made for Source table "  ~ int_tbl_identifier ~ " as ODATE value is not a date", info=true) }}
      {% else %}

        {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), 0, 0, 0, 'Source TABLE EXISTS', 'P' ) }}

      {% endif %}
    {% else %}
      {{ raise_validation_error( "Source table "  + int_tbl_identifier + " is not available in the database.schema " +  src_db_nm + "." + src_sch_nm) }}
      {% set validation_flag = validation_flag+1 %}
    {% endif %}
  {% endif %}



{{ log ("Number of validation errors: "~validation_flag,info=true )}}

{% if validation_flag > 0 %}
  {{ log ("Input parameter validation failed!!!.Please check above logs for more information", info=true)}}
  {{ log ("#####################################################################"~full_refresh_flag,info=true )}}
  {{ log ("######################INPUT PARAMETERS VALIDATION END################"~full_refresh_flag,info=true )}}
  {{ log ("#####################################################################"~full_refresh_flag,info=true )}}
  {{ exceptions.raise_compiler_error( "Input parameters validation failed.Check above logs for more information") }}

{% else %}
  {{ log ("Input parameter validation passed!!!", info=true)}}
  {{ log ("#####################################################################"~full_refresh_flag,info=true )}}
  {{ log ("######################INPUT PARAMETERS VALIDATION END################"~full_refresh_flag,info=true )}}
  {{ log ("#####################################################################"~full_refresh_flag,info=true )}}
{% endif %}

{% set tgttablerelattion = api.Relation.create(
       database = tgt_db_nm,
       schema = tgt_sch_nm,
       identifier = target_table_name) -%}

{{ run_hooks(pre_hooks, inside_transaction=True) }}

{% if  full_refresh_flag == 'Y' %}
----########FULL REFRESH############
  {{ log(" \nPerforming full refresh " ~ full_refresh_flag  ~ " for table " ~ tgt_db_nm ~ tgt_sch_nm ~ target_table_name~ "\n" ,info=true) }}

  {% set trunc_tgt_tbl %}
  TRUNCATE table {{ tgt_db_nm }}.{{tgt_sch_nm }}.{{target_table_name}}
  {%- endset -%}

  {{ log("Truncating table  " ~ tgt_db_nm ~"." ~  tgt_sch_nm ~"." ~  target_table_name ,info=true) }}
  {{ run_generic_sql(trunc_tgt_tbl,tgt_db_nm,tgt_sch_nm,'Truncating the target table') }}
  {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), 0, 0, 0, 'Truncated the table '+tgt_db_nm + "." +  tgt_sch_nm +"." + target_table_name  , 'P' ) }}


  {{ log("\nExecuting the following query: ",info=true) }}
  {{ log("INSERT INTO " ~ tgt_db_nm ~"." ~ tgt_sch_nm ~"." ~ target_table_name~" " ~ sql  ~"\n" , info= true ) }}

  {%- call statement('main',fetch_result=true) -%}
  INSERT INTO {{tgt_db_nm}}.{{tgt_sch_nm}}.{{target_table_name}} {{ sql }};
  {%- endcall -%}

  {% set insertcounts = load_result('main')['data'][0][0]  %}
  {{ log("Number of records inserted = " ~ insertcounts, info = true) }}
  {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), insertcounts, 0, 0, "INSERT statement Completed into "+tgt_db_nm + "." +  tgt_sch_nm +"." + target_table_name +". Check INS_REC_CNTS column for more details" , 'P' ) }}


{% else %}

----########SNAPSHOT REFRESH -with date condition############
  {% if snapshot_refresh_flg == 'Y' %}
      {{ log("\nPerforming snapshot type 1 load for table "  ~ tgt_db_nm ~ tgt_sch_nm ~ target_table_name~ "\n" ,info=true) }}
      {% set delete_existing_snapshot %}
      DELETE FROM {{tgt_db_nm}}.{{tgt_sch_nm}}.{{target_table_name}} where {{date_col}} ='{{date_value}}'
      {% endset %}
      {{ run_generic_sql(delete_existing_snapshot,tgt_db_nm,tgt_sch_nm,'Delete existing records for snapshot load') }}


  ----#####REGULAR TYPE 1 LOAD - no date condition##########
  {% else %}
      {{ log("\n\nPerforming regular type 1 load for table "  ~ tgt_db_nm ~"." ~ tgt_sch_nm ~"." ~ target_table_name~ "\n",info=true) }}
      {% set delete_existing %}
      DELETE FROM {{tgt_db_nm}}.{{tgt_sch_nm}}.{{target_table_name}}
      WHERE ({{ key_cols }}) in (select {{key_cols}}
      from {{src_db_nm}}.{{src_sch_nm}}.{{int_tbl_identifier}} )
      {% endset %}
      {{ run_generic_sql(delete_existing,tgt_db_nm,tgt_sch_nm,'Delete existing records for regular type 1 load') }}
  {% endif %}

    {{ log("\nExecuting the following query: ", info = true ) }}
    {{ log("INSERT INTO " ~ tgt_db_nm ~"." ~ tgt_sch_nm ~"." ~ target_table_name~" " ~ sql  ~"\n" , info= true ) }}
    {%- call statement('main') -%}
    INSERT INTO {{tgt_db_nm}}.{{tgt_sch_nm}}.{{target_table_name}} {{ sql }};
    {%- endcall -%}

{% endif %}

{{ run_hooks(post_hooks, inside_transaction=True) }}

{%- do adapter.commit() -%}

{{ log("Query completed successfully.\n\nEnd of materializaion\n " ,info=true) }}
{{ return({'relations': [tgttablerelattion]}) }}

{% endmaterialization %}
