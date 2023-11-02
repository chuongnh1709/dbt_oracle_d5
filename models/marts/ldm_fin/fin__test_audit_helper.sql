/* This not work for Oracle */

{# in dbt Develop #}

{% set old_etl_relation_query %}
    select * from {{ ref('fin__test') }}
    where is_latest
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('fin__test_v') }}
{% endset %}

{% set audit_query = audit_helper.compare_column_values(
    a_query=old_etl_relation_query,
    b_query=new_etl_relation_query,
    primary_key="c1",
    column_to_compare="c2"
) %}

{% set audit_results = run_query(audit_query) %}

{% if execute %}
{% do audit_results.print_table() %}
{% endif %}
