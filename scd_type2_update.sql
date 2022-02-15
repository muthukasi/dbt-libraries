{% macro scd_type2_update(sourcetable, targettable, keycolumn, valuecolumn) %}

  UPDATE {{ targettable }} SET ACTIVE_IND='N', VALID_TO_TS=CAST({{ tz_timestamp(current_timestamp()) }} AS TIMESTAMP)
  FROM {{ sourcetable }}
  WHERE {{ hash_key_value(tablename=targettable, keycolumn=keycolumn) }}={{ sourcetable }}.KEY_HASH_KEY
  AND {{ targettable }}.ACTIVE_IND='Y' AND
  {{ hash_key_value(tablename=targettable, valcolumn=valuecolumn) }}<>{{ sourcetable }}.VALUE_HASH_KEY

{% endmacro %}
