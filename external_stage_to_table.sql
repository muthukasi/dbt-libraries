{% materialization external_stage_to_table, default %}

{% set src_dtl = config.get('src_dtl',default='none') %}
{% set ext_stg_name = config.get('ext_stg_name') %}
{% set ext_stg_file_path = config.get('ext_stg_schema') %}
{% set named_file_format= config.get('named_file_format') %}
{% set def_file_format= config.get('def_file_format') %}
{% set purge_flag= config.get('purge_flag') %}
{% set skip_header= config.get('skip_header') %}
{% set force_flag= config.get('force_flag') %}

{% set trgt_stgtable  = config.get('trgt_stgtable') %}
{% set trgt_dtl=config.get('trgt_dtl',default='none') %}
{% set key_cols = config.get('key_cols') %}
{% set odate = config.get('odate') %}
{% set materializationname = config.get('materialized') %}


{% set modelexestrttime = run_started_at.astimezone(modules.pytz.timezone(var("time_zone"))) -%}

-- Source schema in which the stage is created
{% set source_details = (source(src_dtl  ,'dual'))   %}
{% set stgdatabase= source_details['database'] %}
{% set stgschema= source_details['schema'] %}


--Target schema of trgt_stgtable
{% set target_details = (source( trgt_dtl ,'dual'))  %}
{% set tgt_db_nm= target_details['database'] %}
{% set tgt_sch_nm= target_details['schema'] %}


{{ log ("#####################################################################",info=true )}}
{{ log ("######################INPUT PARAMETERS VALIDATION START##############",info=true )}}
{{ log ("#####################################################################",info=true )}}
{% set validation_flag = 0 %}



{% set date = modules.datetime.date %}
{% set is_date = date.fromisoformat(odate)   %}

{{ log("is_date: " ~ is_date , info = true ) }}


{% if is_date is none %}
  {{ log("VALIDATION ERROR for odate   -- to be handled later .. value passed is not a date", info= true) }}
{% else %}
  {{ log("Odate value passed is a date type:" ~ odate , info= true) }}
{% endif %}



-- Check if source stage is available
{% set source_stage_availability %}
      select stage_type
      from {{stgdatabase}}."INFORMATION_SCHEMA"."STAGES"
      where UPPER(stage_name)=UPPER('{{ext_stg_name}}')
      and STAGE_SCHEMA='{{stgschema}}'
{%- endset -%}
{% set results_tgt = run_query(source_stage_availability) %}


{% set stage_type = results_tgt.columns[0].values()[0] %}


{% if results_tgt is none %}
  {{ raise_validation_error("Source DB "  + stgdatabase + " unavailable" ) }}
  {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'SOURCE DB DOESNOT EXIST', 'F' ) }}
  {% set validation_flag = validation_flag+1 %}
{% else %}
  {% if stage_type =="Internal Named" or stage_type ==  "External Named" %}
    {{ log ("Stage_type="~stage_type, info=true )}}
    {{ log("  Source stage '"  ~ ext_stg_name ~ "' is available in the database.schema " ~  stgdatabase ~ "." ~ stgschema, info=true) }}
    {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), 0, 0, 0, 'Source stage EXISTS', 'P' ) }}
    {% if stage_type =="Internal Named" %}
      {{ log( "Internal Named stage", info = true) }}
      {% set purge_ind = true %}
    {% else %}

      {{ log ("purge_flag="~purge_flag , info = true )}}
      {% set purge_ind =  purge_flag %}
    {% endif %}

    --- List to get the md5 value on the file
    {% set check_stage_exists %}
          list @{{ext_stg_name}}/{{ext_stg_file_path}};
    {% endset %}


    {% set results = run_query(check_stage_exists) %}

    {% set file_name = results.columns[0].values()[0] %}
    {%set size=results.columns[1].values()[0] | as_number  %}
    {%set md5_val=results.columns[2].values()[0] %}
    {%set last_updated_ts=results.columns[3].values()[0] %}

    {{ log ("  file_name=" ~ file_name ~"." , info =true )}}
    {{ log ("  md5_val= ." ~ md5_val ~"." , info =false )}}
    {{ log ("  last_updated_ts= "~ last_updated_ts  , info=false )}}
    {{ log ("  Check if the value of size="~ size~ " is a number. The process will fail if source file does not exists", info =true  )}}
    {% if size >= 0 %}
      {{ log("  The external file "~ file_name ~ " exists in " ~ stgdatabase ~"."~ stgschema ~"."~ ext_stg_name , info=true) }}
      {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), 0, 0, 0, 'External/Internal file exists', 'P' ) }}
    {% endif %}


    {{ log ("  md5_val= "~ md5_val  , info =true )}}
    {{ log ("  last_updated_ts= "~ last_updated_ts  , info =true )}}




  {% else %}
    {{ raise_validation_error( "Source stage "  + ext_stg_name + " is not available in the database.schema " +  stgdatabase + "." + stgschema) }}
    {% set validation_flag = validation_flag+1 %}
  {% endif %}
{% endif %}





-- Check if target table is available
{% set target_table_availability %}
  select count(*) from {{tgt_db_nm}}."INFORMATION_SCHEMA"."TABLES"
  where upper(table_name)=UPPER('{{trgt_stgtable}}')
  and table_schema='{{tgt_sch_nm}}'
{%- endset -%}

{% set results_tgt = run_generic_sql(target_table_availability ,tgt_db_nm,tgt_sch_nm,'Target Table Availability check in DBT','Y') %}

{% if results_tgt is none %}
  {{ raise_validation_error("Target DB "  + tgt_db_nm + " unavailable" ) }}
  {{ audit_insert(invocation_id,odate,'ABORTED', modelexestrttime, current_timestamp(), 0, 0, 0, 'TARGET DB DOESNOT EXIST', 'F' ) }}
  {% set validation_flag = validation_flag+1 %}
{% else %}
  {% if results_tgt == 1 %}
    {{ log("  Target Table '"  ~ trgt_stgtable ~ "' is available in the database.schema " ~  tgt_db_nm ~ "." ~ tgt_sch_nm, info=true) }}
    {{ audit_insert(invocation_id,odate,'SUCCESS', modelexestrttime, current_timestamp(), 0, 0, 0, 'Target Table EXISTS', 'P' ) }}
  {% else %}
    {{ raise_validation_error( "Target Table "  + trgt_stgtable + " is not available in the database.schema " +  tgt_db_nm + "." + tgt_sch_nm ) }}
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





{% set trgt_stgtable = api.Relation.create(
       database = tgt_db_nm,
       schema = tgt_sch_nm,
       identifier = trgt_stgtable) -%}
%}

{% set core_logic %}
COPY INTO {{trgt_stgtable}} FROM
 ( {{ sql }} )
{% if named_file_format =='' -%}
  file_format = (type= {{def_file_format}} SKIP_HEADER = {{skip_header}} )
{% else -%}
  file_format = '{{named_file_format}}'
{% endif -%}
FORCE = {{force_flag}}
PURGE = {{purge_ind}}
{% endset %}

{{ log("Executing the following statement", info = true)}}
{{ log(core_logic, info = true)}}


{% call statement('main',fetch_result=true) -%}
{{core_logic}}
{% endcall %}

{% do adapter.commit() %}
{{ return({'relations': [trgt_stgtable]}) }}
{% endmaterialization %}
--Check if the file path or external file exists using a list command
