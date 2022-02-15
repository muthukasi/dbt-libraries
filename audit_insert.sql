{% macro audit_insert (INVOCATION_ID, ETL_RUN_DT, MODEL_STATUS,
  MODEL_EXEC_START_TIME, MODEL_EXEC_END_TIME,INS_REC_CNTS,UPD_REC_CNTS,DEL_REC_CNTS,
  AUDIT_CHK_DESC,AUDIT_CHK_FL) %}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- call statement() -%}

    INSERT INTO {{ source('AUDIT','M_ETL_AUDIT_CHECK') }}
    (AUDIT_SK, ETL_RUN_DT, INVOCATION_ID, MODEL_NAME, MODEL_STATUS, MODEL_EXEC_START_TIME,
      MODEL_EXEC_END_TIME, INS_REC_CNTS, UPD_REC_CNTS, DEL_REC_CNTS, AUDIT_CHK_DESC, AUDIT_CHK_FL)
    SELECT AUDIT_SK+1 AS AUDIT_SK,CAST('{{ ETL_RUN_DT }}' AS DATE) AS ETL_RUN_DT,
    '{{ INVOCATION_ID }}' AS INVOCATION_ID,
    '{{ this.table }}' AS MODEL_NAME, '{{ MODEL_STATUS }}' AS MODEL_STATUS,
    CAST('{{ MODEL_EXEC_START_TIME }}' AS TIMESTAMP) AS MODEL_EXEC_START_TIME,
    CAST({{ tz_timestamp(MODEL_EXEC_END_TIME) }} AS TIMESTAMP) AS MODEL_EXEC_END_TIME,
    {{ INS_REC_CNTS }} AS INS_REC_CNTS, {{ UPD_REC_CNTS }} AS UPD_REC_CNTS,
    {{ DEL_REC_CNTS }} AS DEL_REC_CNTS, '{{ AUDIT_CHK_DESC }}' AS AUDIT_CHK_DESC,
    '{{ AUDIT_CHK_FL }}' AS AUDIT_CHK_FL
    FROM ( SELECT COALESCE(MAX(AUDIT_SK),0) AS AUDIT_SK
    FROM {{ source('AUDIT','M_ETL_AUDIT_CHECK') }} )

  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

{% endmacro %}
