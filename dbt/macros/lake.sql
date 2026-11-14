{#
  Where the lake is. s3://sme-lake by default (Floci here, AWS one day); a local folder in continuous
  integration, where there is no S3 (LAKE_ROOT=data/ci-lake, see .github/workflows/ci.yml).
  The bronze source (sources.yml) and every external model build their paths from it, so the same models
  run in both places without any other change.
#}
{% macro lake_root() %}
    {{ return(env_var('LAKE_ROOT', 's3://sme-lake')) }}
{% endmacro %}

{# File of an external model: one folder per table, as Glue and Athena need (M9). #}
{% macro lake_path(layer, model_name) %}
    {{ return(lake_root() ~ '/' ~ layer ~ '/' ~ model_name ~ '/' ~ model_name ~ '.parquet') }}
{% endmacro %}
