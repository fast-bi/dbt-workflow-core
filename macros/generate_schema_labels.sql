{% macro update_dataset_labels(dataset_name, project, key, value) %}

   {% set update_labels_query %}
        ALTER schema `{{project}}`.{{dataset_name}}
        SET OPTIONS (
            labels=[("{{key}}", "{{value}}")]
        )
   {% endset %}

   {% set get_labels %}
        SELECT
            option_value
        FROM
            `{{project}}`.INFORMATION_SCHEMA.SCHEMATA_OPTIONS
        WHERE
          schema_name="{{dataset_name}}"
          and option_name="labels"
  {% endset%}

  {% do run_query(update_labels_query) %}
  {% set result =  run_query(get_labels).columns[0].values() %}
  {% set key, value = result[0].split('(')[1].split(')')[0].split(', ') %}
  {% do log("Labels updated for dataset '{}': ({}: {})".format(dataset_name, key.strip('\"'), value.strip('\"')), True) %}

{% endmacro %}