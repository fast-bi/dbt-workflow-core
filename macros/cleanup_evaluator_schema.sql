-- Macro: cleanup_evaluator_schema
-- =============================================================================
-- Drops the dbt-project-evaluator CI schema after quality checks complete.
-- Called by branch_workflow.yaml check-dbt-quality step to prevent warehouse
-- pollution from evaluator models created on every branch commit.
--
-- Deployment: copy this file to dbt-workflow-core/macros/
--             so it is available in the Docker image at
--             /usr/app/dbt/macros/cleanup_evaluator_schema.sql
--
-- Usage (from workflow):
--   dbt run-operation cleanup_evaluator_schema \
--     --args '{"schema_name": "dbt_evaluator_ci", "dry_run": False}'
-- =============================================================================

{% macro cleanup_evaluator_schema(schema_name='dbt_evaluator_ci', dry_run=True) %}

  {% if dry_run %}

    {{ log("DRY RUN: Would drop schema '" ~ schema_name ~ "' (no changes made)", info=True) }}

  {% else %}

    {{ log("Dropping evaluator schema: " ~ schema_name, info=True) }}

    {%- set schema_relation = api.Relation.create(
        database = target.database,
        schema   = schema_name
    ) -%}

    {%- call statement('drop_evaluator_schema', fetch_result=False) -%}
      drop schema if exists {{ adapter.quote(schema_name) }} cascade
    {%- endcall -%}

    {{ log("Schema '" ~ schema_name ~ "' dropped successfully", info=True) }}

  {% endif %}

{% endmacro %}
