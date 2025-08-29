{% macro create_dataset_bigquery(project_id, dataset_name) %}
    {% set query %}
    CREATE SCHEMA IF NOT EXISTS `{{ project_id }}.{{ dataset_name }}`;
    {% endset %}
    {{ log("Creating dataset in Data Warehouse: " ~ dataset_name, info=True) }}
    {{ run_query(query) }}
{% endmacro %}

{% macro create_dataset_snowflake(project_id, dataset_name) %}
    {% set query %}
    CREATE SCHEMA IF NOT EXISTS {{ project_id }}.{{ dataset_name }};
    {% endset %}
    {{ log("Creating dataset in Data Warehouse: " ~ dataset_name, info=True) }}
    {{ run_query(query) }}
{% endmacro %}

{% macro create_dataset_fabric(project_id, dataset_name) %}
    {% set query %}
    IF NOT EXISTS (
        SELECT * FROM sys.schemas WHERE name = '{{ dataset_name }}'
    )
    BEGIN
        EXEC('CREATE SCHEMA {{ dataset_name }}');
    END;
    {% endset %}
    {{ log("Creating schema in Fabric if not exists: " ~ dataset_name, info=True) }}
    {{ run_query(query) }}
{% endmacro %}

{% macro create_dataset_redshift(project_id, dataset_name) %}
    {% set query %}
    CREATE SCHEMA IF NOT EXISTS "{{ dataset_name }}";
    {% endset %}
    {{ log("Creating schema in Redshift: " ~ dataset_name, info=True) }}
    {{ run_query(query) }}
{% endmacro %}

{% macro create_dataset_postgres(project_id, dataset_name) %}
    {% set query %}
    CREATE SCHEMA IF NOT EXISTS "{{ dataset_name }}";
    {% endset %}
    {{ log("Creating schema in Postgres: " ~ dataset_name, info=True) }}
    {{ run_query(query) }}
{% endmacro %}

{% macro set_data_tablesample(project_id, source_dataset, sample_dataset, table_name, percent_sample) -%}
    {% set engine = target.type -%}
    {% if engine == 'bigquery' -%}
    {% set query %}
    CREATE OR REPLACE TABLE `{{ project_id }}.{{ sample_dataset }}.{{ table_name }}` AS (
    SELECT *
    FROM `{{ project_id }}.{{ source_dataset }}.{{ table_name }}`
    TABLESAMPLE SYSTEM ({{ percent_sample }} PERCENT));
    {% endset %}
    {% elif engine == 'postgres' %}
    {% set query %}
    CREATE TABLE "{{ sample_dataset }}"."{{ table_name }}" AS
    SELECT *
    FROM "{{ source_dataset }}"."{{ table_name }}"
    TABLESAMPLE SYSTEM ({{ percent_sample }});
    {% endset %}
    {% elif engine == 'redshift' -%}
    {% set query %}
    CREATE TABLE "{{ sample_dataset }}"."{{ table_name }}" AS
    SELECT *
    FROM "{{ source_dataset }}"."{{ table_name }}"
    WHERE RANDOM() < {{ percent_sample }} / 100;
    {% endset %}

{% elif engine == 'snowflake' -%}
    {% set table_name_upper_case = table_name | upper %}
    {% set query %}
    CREATE OR REPLACE TABLE {{ sample_dataset }}."{{ table_name_upper_case }}" AS
    SELECT *
    FROM {{ source_dataset }}."{{ table_name_upper_case }}"
    SAMPLE ({{ percent_sample }});
    {% endset %}

    {% elif engine == 'fabric' -%}
    {% set query %}
    CREATE TABLE {{ sample_dataset }}."{{ table_name }}" AS
    SELECT *
    FROM {{ source_dataset }}.{{ table_name }}
    TABLESAMPLE ({{ percent_sample }});
    {% endset %}
    {% else -%}
    -- Default behavior, no limit applied
    {{ log("Default behavior, no limit applied", info=True) }}
    {% endif -%}
    {{ run_query(query) }}
{%- endmacro %}

{% macro create_samples_for_dataset(percent_sample=10) %}
    {% set engine = target.type -%}
    {% if target.name == 'e2e' or target.name == 'test' or target.name == 'e2e_test' %}
        -- Step 1: Get the list of tables from the source.yml
        {% set source_nodes = graph.sources %}
        {% for node_key, node_value in source_nodes.items() %}
            -- take dataset from source.yml file
            {% set source_dataset = node_key.split('.')[-2] %}
            {% set sample_database = target.database %}
            {% if engine == 'bigquery' %}
                {% set sample_dataset = target.dataset +"_"+ source_dataset %}
                {{ create_dataset_bigquery(sample_database, sample_dataset) }}
            {%elif engine == 'snowflake' %}
                {% set sample_dataset = target.schema +"_"+ source_dataset %}
                {{ create_dataset_snowflake(sample_database, sample_dataset) }}
            {%elif engine == 'postgres' %}
            -- need to provide correct sample_dataset
            {% set sample_dataset = target.schema +"_"+ source_dataset %}
            {{ create_dataset_postgres(sample_database, sample_dataset) }}
            {%elif engine == 'redshift' %}
            -- need to provide correct sample_dataset
            {% set sample_dataset = target.schema +"_"+ source_dataset %}
            {{ create_dataset_redshift(sample_database, sample_dataset) }}
            {%elif engine == 'fabric' %}
            -- need to provide correct sample_dataset
            {% set sample_dataset = target.schema +"_"+ source_dataset %}
            {{ create_dataset_fabric(sample_database, sample_dataset) }}
            {% endif %}
            -- Check if table has identifier then take table.identifier otherwise take table.name
            {% set table_name = node_value.get('identifier', node_value.get('name')) %}
            {% set skipped_table_name = node_value.get('loader', '').lower() %}
            {% if skipped_table_name == 'ml' %}
                {{ log("Skipping sample table creation for ml table " ~ sample_dataset ~ "." ~table_name, info=True) }}
            {% elif '_*' in table_name %}
                {{ log("Skipping sample table creation for partitioned table " ~ sample_dataset ~ "." ~table_name, info=True) }}
            {% else %}
                {{ log("Creating sampled table: " ~ sample_dataset ~ "." ~table_name, info=True) }}
                {{ set_data_tablesample(sample_database, source_dataset, sample_dataset, table_name, percent_sample) }}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("Skipping sample table creation for non-test environments.", info=True) }}
    {% endif %}
{% endmacro %}

{% macro delete_dataset(project_id, dataset_name) %}
    {% set engine = target.type %}
    {% if engine == 'bigquery' %}
        {% set query %}
            DROP SCHEMA IF EXISTS `{{ project_id }}.{{ dataset_name }}` CASCADE;
        {% endset %}
    {% elif engine == 'postgres' %}
        {% set query %}
            DROP SCHEMA IF EXISTS "{{ dataset_name }}" CASCADE;
        {% endset %}
    {% elif target.type in ['redshift', 'snowflake', 'fabric'] %}
    {% set query %}
        DROP SCHEMA IF EXISTS {{ project_id }}.{{ dataset_name }} CASCADE;
    {% endset %}
    {% else %}
        -- Default behavior, do nothing
        {% set query %}{% endset %}
    {% endif %}
    {% if query %}
        {{ log("Deleting dataset: " ~ dataset_name, info=True) }}
        {{ run_query(query) }}
    {% else %}
        {{ log("No action taken for engine: " ~ engine, info=True) }}
    {% endif %}
{% endmacro %}

{% macro delete_samples_for_dataset() %}
    {% if target.name == 'e2e' or target.name == 'test' or target.name == 'e2e_test' %}
        {% set engine = target.type -%}
        {% if engine == 'bigquery' %}
        {% set base_schema = target.dataset %}
        {% else %}
        {% set base_schema = target.schema %}
        {% endif %}
        -- Get the list of source datasets from source.yml
        {% set source_nodes = graph.sources %}
        {% set sample_database = target.database %}
        {% set processed_datasets = [] %}
        {% for node_key, node_value in source_nodes.items() %}

            {% set source_dataset = node_key.split('.')[-2] %}
            {% set sample_dataset = base_schema ~ "_" ~ source_dataset %}
            {% if sample_dataset not in processed_datasets %}
                {{ delete_dataset(sample_database, sample_dataset) }}
                {% do processed_datasets.append(sample_dataset) %}
            {% endif %}
        {% endfor %}
    {% else %}
        {{ log("Skipping sample dataset deletion for non-test environments.", info=True) }}
    {% endif %}
{% endmacro %}
