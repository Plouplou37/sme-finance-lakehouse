{#
  SQL that differs from one engine to another lives here, behind adapter.dispatch:
  at compile time dbt picks <adapter>__<macro> (duckdb__…, bigquery__…), else default__….
  Moving to another engine means adding its version here, not rewriting every model.
  Only the DuckDB versions are tested; the BigQuery ones show the idea.
#}

{# Text value of a top-level field of a JSON string (NULL if the field is missing). #}
{% macro json_text(json_column, field) %}
    {{ return(adapter.dispatch('json_text')(json_column, field)) }}
{% endmacro %}

{% macro default__json_text(json_column, field) %}
    json_extract_string({{ json_column }}, '$.{{ field }}')
{% endmacro %}

{% macro bigquery__json_text(json_column, field) %}
    json_value({{ json_column }}, '$.{{ field }}')
{% endmacro %}


{#
  Surrogate key: a hash of the given columns. Stable from one run to the next (unlike a row number,
  which would shift when a new row is added), so facts keep pointing at the right dimension row.
#}
{% macro surrogate_key(columns) %}
    md5(concat_ws('|', {% for column in columns %}cast({{ column }} as varchar){% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}


{# Conversion that returns NULL instead of failing when the value cannot be converted. #}
{% macro safe_cast_to(expression, data_type) %}
    {{ return(adapter.dispatch('safe_cast_to')(expression, data_type)) }}
{% endmacro %}

{% macro default__safe_cast_to(expression, data_type) %}
    try_cast({{ expression }} as {{ data_type }})
{% endmacro %}

{% macro bigquery__safe_cast_to(expression, data_type) %}
    safe_cast({{ expression }} as {{ data_type }})
{% endmacro %}
