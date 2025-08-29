-- Removes tables and views from the given run configuration
-- Usage in production:
--    dbt run-operation cleanup_dataset
-- To only see the commands that it is about to perform:
--    dbt run-operation cleanup_dataset --args '{"dry_run": True}'
{% macro cleanup_dbt_dataset(dry_run=False) %}
    {% if execute %}
        {% set current_models=[] %}
        {% set schema_info = {"name": none} %}
        {% do log("Starting cleanup process for database type: " ~ target.type, info=True) %}
        {% do log("Target schema: " ~ target.schema, info=True) %}

        {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed"])%}
            {% do log("Processing node: " ~ node.name ~ " with database: " ~ node.database ~ " and schema: " ~ node.schema, info=True) %}
            {% do current_models.append(node.name.upper()) %}
            {% if schema_info.name is none %}
                {% do log("Setting schema name to: " ~ node.schema, info=True) %}
                {% do schema_info.update({"name": node.schema}) %}
            {% endif %}
        {% endfor %}

        {% do log("Using schema name: " ~ schema_info.name, info=True) %}
        {% do log("Current models to keep: " ~ current_models, info=True) %}
    {% endif %}

    {% set engine = target.type %}
    {% if engine == 'bigquery' %}
        {% if execute %}
            {% set current_model_locations={} %}

            {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed"])%}
                {% if not node.database in current_model_locations %}
                    {% do current_model_locations.update({node.database: {}}) %}
                {% endif %}
                {% if not node.schema in current_model_locations[node.database] %}
                    {% do current_model_locations[node.database].update({node.schema: []}) %}
                {% endif %}
                {% set table_name = node.alias if node.alias else node.name %}
                {% do current_model_locations[node.database][node.schema].append(table_name) %}
            {% endfor %}
        {% endif %}

        {% set cleanup_query %}
            with models_to_drop as (
                {% for database in current_model_locations.keys() %}
                    {% if loop.index > 1 %}union all{% endif %}
                    {% for dataset, tables  in current_model_locations[database].items() %}
                        {% if loop.index > 1 %}union all{% endif %}
                        select
                            table_type,
                            table_catalog,
                            table_schema,
                            table_name,
                            case
                                when table_type = 'BASE TABLE' then 'TABLE'
                                when table_type = 'VIEW' then 'VIEW'
                            end as relation_type,
                            array_to_string([table_catalog, table_schema, table_name], '.') as relation_name,
                            array_to_string([table_catalog, table_schema], '.') as schema_relation_name
                        from `{{ dataset }}.INFORMATION_SCHEMA.TABLES`
                        where table_name in ({% for table in tables %}'{{ table }}'{% if not loop.last %}, {% endif %}{% endfor %})
                    {% endfor %}
                {% endfor %}
            ),
            drop_commands as (
                select 'DROP ' || relation_type || ' IF EXISTS `' || relation_name || '`;' as command
                from models_to_drop
                UNION ALL
                select 'DROP SCHEMA IF EXISTS `' || schema_relation_name || '` CASCADE;' as command
                from models_to_drop
            )
            select DISTINCT command
            from drop_commands
            where command is not null
            order by 1 desc;
        {% endset %}
    {% elif engine == 'snowflake' %}
        {% if schema_info.name is none %}
            {% do log("No schema name found in nodes", info=True) %}
            {% set cleanup_query = "" %}
        {% else %}
            {% set cleanup_query %}
                with models_to_drop as (
                    select DISTINCT
                        case 
                            when table_type = 'BASE TABLE' then 'TABLE'
                            when table_type = 'VIEW' then 'VIEW'
                        end as relation_type,
                        '"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"' as relation_name,
                        '"' || table_catalog || '"."' || table_schema || '"' as schema_relation_name
                    from 
                        {{ target.database }}.INFORMATION_SCHEMA.TABLES
                    where 
                        table_schema = '{{ schema_info.name | upper }}'
                    union all
                    select DISTINCT
                        'VIEW' as relation_type,
                        '"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"' as relation_name,
                        '"' || table_catalog || '"."' || table_schema || '"' as schema_relation_name
                    from 
                        {{ target.database }}.INFORMATION_SCHEMA.VIEWS
                    where 
                        table_schema = '{{ schema_info.name | upper }}'
                )
                select DISTINCT
                    'DROP ' || relation_type || ' IF EXISTS ' || relation_name || ';' as command
                from 
                    models_to_drop
                union all
                select DISTINCT
                    'DROP SCHEMA IF EXISTS ' || schema_relation_name || ' CASCADE;' as command
                from 
                    models_to_drop
                where 
                    command is not null
                order by 
                    1 desc;
            {% endset %}
        {% endif %}
    {% elif engine == 'redshift' %}
        {% if schema_info.name is none %}
            {% do log("No schema name found in nodes", info=True) %}
            {% set cleanup_query = "" %}
        {% else %}
            {% set cleanup_query %}
                with models_to_drop as (
                    select DISTINCT
                        'TABLE' as relation_type,
                        '"' || schemaname || '"."' || tablename || '"' as relation_name,
                        '"' || schemaname || '"' as schema_relation_name
                    from 
                        pg_catalog.pg_tables
                    where 
                        schemaname = '{{ schema_info.name | lower }}'
                    union all
                    select DISTINCT
                        'VIEW' as relation_type,
                        '"' || schemaname || '"."' || viewname || '"' as relation_name,
                        '"' || schemaname || '"' as schema_relation_name
                    from 
                        pg_catalog.pg_views
                    where 
                        schemaname = '{{ schema_info.name | lower }}'
                )
                select DISTINCT
                    'DROP ' || relation_type || ' IF EXISTS ' || relation_name || ';' as command
                from 
                    models_to_drop
                union all
                select DISTINCT
                    'DROP SCHEMA IF EXISTS ' || schema_relation_name || ' CASCADE;' as command
                from 
                    models_to_drop
                where 
                    command is not null
                order by 
                    1 desc;
            {% endset %}
        {% endif %}
    {% elif engine == 'fabric' %}
        {% if schema_info.name is none %}
            {% do log("No schema name found in nodes", info=True) %}
            {% set cleanup_query = "" %}
        {% else %}
            {% set cleanup_query %}
                with models_to_drop as (
                    select DISTINCT
                        case 
                            when type = 'U' then 'TABLE'
                            when type = 'V' then 'VIEW'
                        end as relation_type,
                        '[' || SCHEMA_NAME(schema_id) + '].[' + name + ']' as relation_name,
                        '[' || SCHEMA_NAME(schema_id) + ']' as schema_relation_name
                    from 
                        sys.objects
                    where 
                        schema_id = SCHEMA_ID('{{ schema_info.name }}')
                        and type in ('U', 'V')
                )
                select DISTINCT
                    'DROP ' || relation_type || ' IF EXISTS ' || relation_name + ';' as command
                from 
                    models_to_drop
                union all
                select DISTINCT
                    'DROP SCHEMA IF EXISTS ' || schema_relation_name + ';' as command
                from 
                    models_to_drop
                where 
                    command is not null
                order by 
                    1 desc;
            {% endset %}
        {% endif %}
    {% else %}
        {% do log("Unsupported database type: " ~ engine, info=True) %}
        {% set cleanup_query = "" %}
    {% endif %}

    {% if cleanup_query %}
        {% do log("Generated cleanup query: " ~ cleanup_query, info=True) %}
        {% set drop_commands = run_query(cleanup_query).columns[0].values() %}
        {% if drop_commands %}
            {% for drop_command in drop_commands %}
                {% do log(drop_command, True) %}
                {% if dry_run | as_bool == False %}
                    {% do run_query(drop_command) %}
                {% endif %}
            {% endfor %}
        {% else %}
            {% do log('No relations to clean.', True) %}
        {% endif %}
    {% endif %}
{%- endmacro -%}

{% macro set_schema_name(schema) %}
    {% set schema_name = schema %}
{% endmacro %}