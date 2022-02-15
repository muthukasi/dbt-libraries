{% macro raise_validation_error(message) %}

{{log  ("   ------------------", info= true) }}
{{ log ("   |VALIDATION ERROR|::: " ~ message , info= true) }}
{{log  ("   ------------------", info= true) }}

{% endmacro %}
