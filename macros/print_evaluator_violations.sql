{% macro print_evaluator_violations(schema_name='dbt_evaluator_ci') %}
  {%- if execute -%}

    {#
      Queries every dbt-project-evaluator fct_ table and prints a clean,
      per-model violation report. Called after `dbt build --select package:dbt_project_evaluator`.
      Tables are grouped by category so the output is easy to scan.

      Usage:
        dbt run-operation print_evaluator_violations \
          --args '{"schema_name": "dbt_evaluator_ci"}' \
          --target ci
    #}

    {%- set categories = [
      ('DOCUMENTATION', [
        'fct_undocumented_models',
        'fct_undocumented_sources',
        'fct_undocumented_source_tables',
        'fct_documentation_coverage'
      ]),
      ('TESTING', [
        'fct_missing_primary_key_tests',
        'fct_test_coverage',
        'fct_sources_without_freshness'
      ]),
      ('DAG STRUCTURE', [
        'fct_direct_join_to_source',
        'fct_duplicate_sources',
        'fct_hard_coded_references',
        'fct_marts_or_intermediate_dependent_on_source',
        'fct_model_fanout',
        'fct_multiple_sources_joined',
        'fct_rejoining_of_upstream_concepts',
        'fct_root_models',
        'fct_source_fanout',
        'fct_staging_dependent_on_marts_or_intermediate',
        'fct_staging_dependent_on_staging',
        'fct_too_many_joins',
        'fct_unused_sources'
      ]),
      ('PROJECT STRUCTURE', [
        'fct_model_directories',
        'fct_model_naming_conventions',
        'fct_source_directories',
        'fct_test_directories'
      ]),
      ('PERFORMANCE', [
        'fct_chained_views_dependencies',
        'fct_exposure_parents_materializations'
      ]),
      ('GOVERNANCE', [
        'fct_exposures_dependent_on_private_models',
        'fct_public_models_without_contract',
        'fct_undocumented_public_models'
      ])
    ] -%}

    {%- set ns = namespace(total=0, any_found=false) -%}

    {{ print("") }}
    {{ print("=" * 70) }}
    {{ print("  dbt-project-evaluator  ·  VIOLATION REPORT") }}
    {{ print("  schema: " ~ schema_name) }}
    {{ print("=" * 70) }}

    {%- for category, tables in categories -%}
      {%- set cat_printed = [] -%}

      {%- for table in tables -%}
        {%- set rel = adapter.get_relation(
              database=target.database,
              schema=schema_name,
              identifier=table) -%}

        {%- if rel -%}
          {%- set results = run_query('SELECT * FROM ' ~ rel ~ ' LIMIT 100') -%}

          {%- if results and results.rows | length > 0 -%}

            {%- if cat_printed | length == 0 -%}
              {{ print("") }}
              {{ print("  ❌  " ~ category) }}
              {{ print("  " ~ ("─" * 66)) }}
              {%- do cat_printed.append(1) -%}
            {%- endif -%}

            {%- set row_count = results.rows | length -%}
            {%- set ns.total = ns.total + row_count -%}
            {%- set ns.any_found = true -%}

            {# Use up to 4 columns — first col is always the resource name #}
            {%- set max_cols = [results.column_names | length, 4] | min -%}
            {%- set cols = results.column_names[:max_cols] -%}

            {# Column widths: 35 chars for first (name), 20 for rest #}
            {%- set widths = [35] + ([20] * (max_cols - 1)) -%}

            {# Header #}
            {{ print("") }}
            {{ print("  " ~ table ~ "  (" ~ row_count ~ " violation(s))") }}
            {%- set header_parts = [] -%}
            {%- for i in range(max_cols) -%}
              {%- set col = cols[i] -%}
              {%- set w = widths[i] -%}
              {%- set padded = col[:w] -%}
              {%- set padded = padded ~ (" " * ([w - padded | length, 0] | max)) -%}
              {%- do header_parts.append(padded) -%}
            {%- endfor -%}
            {{ print("    " ~ header_parts | join("  ")) }}
            {{ print("    " ~ ("·" * 66)) }}

            {# Rows #}
            {%- for row in results -%}
              {%- set row_parts = [] -%}
              {%- for i in range(max_cols) -%}
                {%- set col = cols[i] -%}
                {%- set w = widths[i] -%}
                {%- set val = (row[col] | string)[:w] -%}
                {%- set val = val ~ (" " * ([w - val | length, 0] | max)) -%}
                {%- do row_parts.append(val) -%}
              {%- endfor -%}
              {{ print("    " ~ row_parts | join("  ")) }}
            {%- endfor -%}

            {%- if row_count == 100 -%}
              {{ print("    … (showing first 100 rows)") }}
            {%- endif -%}

          {%- endif -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endfor -%}

    {{ print("") }}
    {{ print("=" * 70) }}
    {%- if ns.any_found -%}
      {{ print("  TOTAL VIOLATIONS: " ~ ns.total) }}
      {{ print("  Fix guide: https://dbt-labs.github.io/dbt-project-evaluator/main/rules/") }}
      {{ print("=" * 70) }}
      {{ print("") }}
      {# Cause the run-operation exit code to be non-zero so CI fails #}
      {{ exceptions.raise_compiler_error("dbt-project-evaluator: " ~ ns.total ~ " violation(s) found. See report above.") }}
    {%- else -%}
      {{ print("  ✅  No violations found — project structure looks healthy!") }}
      {{ print("=" * 70) }}
      {{ print("") }}
    {%- endif -%}

  {%- endif -%}
{% endmacro %}
