#!/bin/bash
#

#Exit on error
set -o errexit

#Error handling
catch() {
    echo 'catching!'
    if [ "$1" != "0" ]; then
    # error handling goes here
    echo "Error $1 occurred on $2"
    exit 1
    fi
}

echo 'Starting dbt labeling add procedure on BigQuery Datasets.'
echo 'Checking dependencies.'

echo "Compiling DBT Project - Manifest."

if [ ! -f "./target/manifest.json" ]; then
    dbt deps --quiet 2>&1
    dbt compile --target "${TARGET}"
else
    echo "Manifest file already exists. Skipping compilation."
fi



key="${DATASET_LABEL_KEY}"
value="${DATASET_LABEL_VALUE}"
echo "End DBT Compilation procedure."

echo "Preparing BQ Dataset list for labeling."
datasets=$(jq '.nodes[] | select(.resource_type == "model" or .resource_type == "seed") | .schema' ./target/manifest.json | sort | uniq | tr -d '"')
database=$(jq '.nodes[] | select(.resource_type == "model" or .resource_type == "seed") | .database' ./target/manifest.json | sort | uniq | tr -d '"')
echo "List on Database ${database}:"
printf '%s\n' "${datasets[@]}"

if test -z "$datasets" 
then
      echo "No Schema's defined in dbt_project.yml file, taking from root - profile.yml"
      datasets=$(grep dataset < profiles.yml | tr -d ' ' | sort | uniq)
fi

echo "Extracting project = database value from DBT Target"

echo "Adding macro file to the dbt project catalog."
macros_path=$(sed -n 's/^macro-paths:[^[]*\[\([^]]*\)\].*/\1/p' dbt_project.yml | tr -d '"')
if [ -d "$macros_path" ]; then
    echo "Directory of dbt macros: $macros_path exists"
else
    echo "Directory $macros_path does not exist"
    mkdir "$macros_path"
fi
cp /usr/app/dbt/macros/generate_schema_labels.sql ./"$macros_path"

for dataset in ${datasets}
do
    echo "Adding label on BigQuery Dataset: " "$dataset"
    dbt run-operation --target "${TARGET}" update_dataset_labels --args '{"dataset_name": "'''"${dataset}"'''", "project": "'''"${database}"'''", "key": "'''"${key}"'''", "value": "'''"${value}"'''"}' || true

done

#Catch the Error
trap 'catch $? $LINENO' EXIT

#Exit