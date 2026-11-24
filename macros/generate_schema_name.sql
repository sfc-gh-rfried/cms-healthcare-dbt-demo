{% macro generate_schema_name(custom_schema_name, node) -%}
    {#- DBT_CMS_SINGLE_SCHEMA=true: build everything in the target schema, for accounts
        where dbt can't create schemas (pre-created, role-per-schema models). -#}
    {%- set single = env_var('DBT_CMS_SINGLE_SCHEMA', 'false') | lower == 'true' -%}
    {%- set prefix = env_var('DBT_CMS_SCHEMA_PREFIX', '') | trim -%}
    {%- if custom_schema_name is none or single -%}
        {{ target.schema }}
    {%- elif prefix -%}
        {{ prefix }}_{{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
