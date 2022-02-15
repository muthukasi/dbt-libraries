{% macro scd_type3_update(sourcetable, targettable, keycolumn, valuecolumndict, efectdatecol, efectdateval, hashkey ) %}

  UPDATE {{ targettable }} SET

  {% for currentcolumn, historycolumnlist in valuecolumndict[0].items() %}

    {% set parsedvaluecolumnlist = historycolumnlist.split(',') %}

	 {% for x in parsedvaluecolumnlist %}

    {{ x }} = CASE WHEN {{ currentcolumn}}_IND='Y' THEN
    {% if not loop.last %}
      {{ parsedvaluecolumnlist[loop.index] }}
    {% else %}
      {{ targettable }}.{{ currentcolumn }}
    {% endif %}
      ELSE {{ targettable }}.{{ x }} END,
   {% endfor %}
   {{ currentcolumn }} = CASE WHEN {{ currentcolumn }}_IND='Y' THEN {{ sourcetable }}.{{ currentcolumn }} ELSE {{ targettable }}.{{ currentcolumn }} END,
   {{ efectdatecol }}=CASE WHEN {{ currentcolumn }}_IND='Y' THEN '{{ efectdateval }}' ELSE {{ targettable }}.{{ efectdatecol }} END
  {% endfor %}
  FROM {{ sourcetable }}
  WHERE {{ hash_key_value(tablename=targettable, keycolumn=keycolumn) }}={{ sourcetable }}.KEY_HASH_KEY

{% endmacro %}
