{% macro chck_col_exists(tbl_nm,
                        db_nm,
                        sch_nm,
                        key_cols)
                        %}
    {{ log("Perform key cols availbility " ~ key_cols ~ "in   :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm ) }}
​
    {%- set key_cols_list= key_cols.split(",") -%}
​
    {{ log("key_cols_list " ~ key_cols_list) }}
    {%- set tgt_tbl_cols_list = [] -%}
​
​
      {% for column in key_cols_list %}
        {%- set _ = tgt_tbl_cols_list.append(
            "('" ~ column ~ "')"  ) -%}
      {% endfor %}
​
      {%- set tgt_tbl_cols_list_new = tgt_tbl_cols_list|join(', ') -%}
​
    {%- set chk_col_query -%}
      select k.column_name from
      (select col_nm  as column_name from (values {{tgt_tbl_cols_list_new}} ) as T1 (COL_NM) ) as K
      left join ( select column_name,table_name from {{db_nm}}.information_schema.columns where table_name=upper('{{tbl_nm}}')) I
       on upper(k.column_name)=upper(I.column_name)  where I.column_name is null
    {%- endset -%}
​
    {{ log("running the following query to check the column availbility  " ~ chk_col_query ) }}
​
    {% set results_1=run_query(chk_col_query) %}
    {% if execute %}
​
          {% set result_count_1 = results_1.columns[0].values() %}
          {{ log(result_count_1[0] ~ "the query result is ") }}
          {% if  result_count_1|length > 0  %}
​
            {{ log("  The following key " ~ result_count_1|length ~ " field(s)  " ~ results_1.columns[0].values() ~ " is/are not avaialble in the table " ~  tbl_nm ~ "." ~ db_nm ~ "."  ~ sch_nm,info=true) }}
​            {{ return(result_count_1|length) }}
          {% else %}
            {{ log("   All the key fields are available in the table "~ tbl_nm ,info=true) }}
​            {{ return(result_count_1|length) }}
          {% endif %}
​
    {% else %}
​        {{ log("Error encountered while querying the table   :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm,info=true) }}
        {{ log("Replace with error exit  :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm,info=true) }}
​        {{ exceptions.raise_compiler_error( "Database or schema do not exist"  ) }}

    {% endif %}
​
{% endmacro %}
