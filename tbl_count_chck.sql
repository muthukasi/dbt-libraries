{% macro tbl_count_chck(tbl_nm,
                        db_nm,
                        sch_nm)
                        %}
    {{ log(" -Perform table count check on  :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm,info=true) }}

    {% set sql_query %}
    select count(*) from {{db_nm}}.{{sch_nm}}.{{tbl_nm}}
    {% endset %}

    {% set results = run_query(sql_query) %}
          {% if execute %}
                {% set int_tbl_count = results.columns[0].values() %}
                {{ log(int_tbl_count[0] ~ "the query result is ") }}
                {% if int_tbl_count[0] > 0  %}
                  {{ log(" -" ~ int_tbl_count[0] ~ " records exists for the table  :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm,info=true) }}
                  {{ return(int_tbl_count[0]) }}
                {% else %}
                  {{ log(" No records available for the table  :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm,info=true) }}
                  {{ return(int_tbl_count[0]) }}
                {% endif %}
          {% else %}
              {{ log("Error encountered while querying the table   :" ~ db_nm ~ "." ~ sch_nm ~ "." ~ tbl_nm,info=true) }}
              {% set message=0 %}
              {{ return (0) }}
          {% endif %}

{% endmacro %}
