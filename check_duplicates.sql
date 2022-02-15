{% macro check_duplicates(materialization, stgdatabase, stgschema, key_columns, activeind ) %}

  SELECT {{ key_columns }},COUNT(*) FROM
  {{ stgdatabase }}.{{ stgschema }}.{{ this.table }}
  {% if materialization == 'scd_type_2_reusable' %}
    WHERE {{ activeind }} = 'Y'
  {% endif %}
  GROUP BY {{ key_columns }} HAVING COUNT(*) > 1

{% endmacro %}
