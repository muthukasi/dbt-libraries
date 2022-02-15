{% macro tz_timestamp(inputtimestamp) %}

  CONVERT_TIMEZONE('{{ var("time_zone") }}',{{ inputtimestamp}} )

{%endmacro%}
