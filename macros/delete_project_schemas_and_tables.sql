-- Removes tables and views from the given run configuration
-- Usage in production:
--    dbt run-operation cleanup_dataset
-- To only see the commands that it is about to perform:
--    dbt run-operation cleanup_dataset --args '{"dry_run": True}'
{% macro cleanup_dbt_dataset(dry_run=False) %}
    {% if execute %}
        {% set current_model_locations={} %}

        {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed", "snapshot"])%}
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
        with models_tables_to_drop as (
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
                        array_to_string([table_catalog, table_schema, table_name], '.') as relation_name
                    from {{ dataset }}.INFORMATION_SCHEMA.TABLES
                    where (table_name in ('{{ "', '".join(tables) }}'))
                {% endfor %}
            {% endfor %}
        ),
        drop_commands_table as (
            select 'drop ' || relation_type || ' `' || relation_name || '`;' as command
            from models_tables_to_drop
        )

        select command
        from drop_commands_table
        -- intentionally exclude unhandled table_types, including 'external table`
        where command is not null

    {% endset %}
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

    {% set cleanup_query_schemas %}
        with models_schema_to_drop as (
            {% for database in current_model_locations.keys() %}
                {% if loop.index > 1 %}union all{% endif %}
                {% for dataset in current_model_locations[database] %}
                {% if loop.index > 1 %}union all{% endif %}
                    select
                    schema_name
                    from {{ database }}.INFORMATION_SCHEMA.SCHEMATA
                    where (schema_name in ('{{ (dataset) }}'))
                {% endfor %}             
            {% endfor %}
        ),
        
        drop_commands_schema as (
            select 'drop SCHEMA' || ' `' || schema_name || '`;' as command
            from models_schema_to_drop
        )

        select command
        from drop_commands_schema
        -- intentionally exclude unhandled table_types, including 'external table`
        where command is not null

    {% endset %}
    {% set drop_commands = run_query(cleanup_query_schemas).columns[0].values() %}
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
{%- endmacro -%}