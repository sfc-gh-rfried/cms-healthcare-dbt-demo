{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set prefix = env_var('DBT_CMS_SCHEMA_PREFIX', '') | trim -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif prefix -%}
        {{ prefix }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
