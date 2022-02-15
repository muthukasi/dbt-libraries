{% macro check_hash(materializationname, stgdatabase, stgschema, sourcetable, tgtdatabase, tgtschema, targettable, loadstrtgy, activeind) %}

{% if materializationname == 'stage_to_integration' %}

  SELECT COUNT(1) FROM {{ stgdatabase }}.{{ stgschema }}.{{ sourcetable }} A
  FULL OUTER JOIN  {{ targettable }} B
  ON A.HASH_KEY=B.HASH_KEY
  WHERE A.HASH_KEY IS NULL OR B.HASH_KEY IS NULL

{% elif materializationname == 'scd_type_2_reusable' and  loadstrtgy == 'FULL' %}

  SELECT COUNT(1) FROM {{ stgdatabase }}.{{ stgschema }}.{{ sourcetable }} A
  FULL OUTER JOIN  {{ tgtdatabase }}.{{ tgtschema }}.{{ targettable }} B
  ON A.HASH_KEY=B.HASH_KEY
  WHERE B.{{ activeind }} = 'Y' AND A.HASH_KEY IS NULL OR B.HASH_KEY IS NULL

{% elif materializationname == 'scd_type_2_reusable' and  loadstrtgy == 'DELTA' %}

  SELECT COUNT(1) FROM {{ stgdatabase }}.{{ stgschema }}.{{ sourcetable }} A WHERE
  NOT EXISTS ( SELECT 1 FROM {{ tgtdatabase }}.{{ tgtschema }}.{{ targettable }} B
  WHERE A.HASH_KEY=B.HASH_KEY AND B.{{ activeind }} = 'Y')

{% elif materializationname == 'scd_type_3_reusable' %}

  SELECT COUNT(1) FROM {{ stgdatabase }}.{{ stgschema }}.{{ sourcetable }} A WHERE
  NOT EXISTS ( SELECT 1 FROM {{ tgtdatabase }}.{{ tgtschema }}.{{ targettable }} B
  WHERE A.HASH_KEY=B.HASH_KEY )

{% else %}

  WITH SOURCE_TABLE AS (
  SELECT {{ hash_dynamic(tablename=sourcetable) }}
  FROM {{ sourcetable }}
  ),
  TARGET_TABLE AS (
  SELECT {{ hash_dynamic(stgdatabase,stgschema,targettable) }}
  FROM {{ stgdatabase }}.{{ stgschema }}.{{ targettable }}
  )
  SELECT COUNT(1) FROM SOURCE_TABLE A
  FULL OUTER JOIN  TARGET_TABLE B
  ON A.HASH_KEY=B.HASH_KEY
  WHERE A.HASH_KEY IS NULL OR B.HASH_KEY IS NULL

{% endif %}

{% endmacro %}
