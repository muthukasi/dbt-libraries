
{% macro run_generic_sql(sql_query,
                        db_nm,
                        sch_nm,
                        message,
                        silent_log
                      )
                        %}
    {% if silent_log =='Y'%}
      {% set flag=false %}
    {% else %}
      {% set flag=true %}
    {% endif %}
    {{ log( " Silent Log flag =" ~ flag ) }}
    {{ log( "  Execute the following query  in " ~ db_nm ~ "." ~ sch_nm ~ ":"  ~ sql_query,info=flag ) }}


    {% set results = run_query(sql_query) %}
      {% if execute %}
          {{ log("  Query completed successfully  in db=" ~ db_nm ~ " and schema=" ~ sch_nm ,info= flag  ) }}

          {% set recs_count = results.columns[0].values() %}
          {{ log("  " ~ recs_count[0] ~ " records processed for " ~ message , info = flag ) }}
          {{ return(recs_count[0]) }}
      {% else %}
          {{ log("  Error encountered while executing the below sql for"~ message ~" in "~ db_nm ~ "." ~ sch_nm ,info=  flag ) }}
          {{ log( "  sql query : " ~ sql_query ,info=true) }}
      {% endif %}


{% endmacro %}
