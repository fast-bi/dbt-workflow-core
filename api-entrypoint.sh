#!/bin/bash

# Define the log file path
LOG_FILE="error.log"
# Function to log errors to the log file
log_error() {
  echo "$1" >&2  # Redirect output to stderr
  echo "$1" >> ${LOG_FILE}
}

# Function to safely read secret files
read_secret() {
    local secret_path="$1"
    if [ -f "$secret_path" ]; then
        cat "$secret_path"
    else
        log_error "Secret file not found: $secret_path"
        return 1
    fi
}

# Define an error handling function
handle_error() {
  local exit_code=$?
  # Perform error handling actions here
  echo "An error occurred. On command running: dbt ${DBT_COMMAND} --select ${MODEL}. Exit code: $exit_code" >> ${LOG_FILE}
  # Additional error handling logic if needed
}

# Set up a trap to call the error handling function on any error or exit
trap 'handle_error' ERR EXIT

# Start The DBT Workload
echo 'Starting DBT Workload.'

#Remove lost+found directory which not allowing clone the repository.
if [[ -f "/data/lost+found" ]]
then
    echo "This lost+found exists on your filesystem."
    rm -rf /data/lost+found
fi

#Checking DNS Resolve.
if test "${GIT_URL}"; then
until curl --output /dev/null --silent --head --fail ${GIT_URL}; do
    printf "Waiting DNS ${GIT_URL} will be resolved!"
    sleep 5
done
else
    echo "GIT address not described!"
fi

#Creating a data directory && start working.
mkdir -p /data/
echo 'Cloning dbt Repo.'
if [[ -n "${GIT_BRANCH}" ]]; then
    echo "Git Branch ${GIT_BRANCH} was specified in env variables, cloning only branch"
    git clone -b "${GIT_BRANCH}" "${GITLINK_SECRET}" /data/dbt/
else
    echo 'Git Clone default branch for dbt project'
    git clone "${GITLINK_SECRET}" /data/dbt/
fi
echo 'Working on dbt directory.'
cd /data/dbt/"${DBT_PROJECT_DIRECTORY}" || exit

#Set the DBT Profile location.
if [[ "${PROFILES_DIR_PATH}" == "default" ]]; then
    echo "dbt profile in default location."
    unset DBT_PROFILES_DIR
elif [[ "${PROFILES_DIR_PATH}" == "repo" ]]; then
    echo "dbt profile in repo location."
    export DBT_PROFILES_DIR='/data/dbt/'
else
    echo "dbt profile is not set, use dbt project location."
    unset DBT_PROFILES_DIR
fi

# Data Warehouse Secrets
if [ "${DATA_WAREHOUSE_SECRET:-}" != "" ] || [ "${DATA_WAREHOUSE_PLATFORM:-}" != "" ]; then
    if [ ! -d "/fastbi/secrets" ]; then
        echo "Secrets directory /fastbi/secrets not found - skipping Data Warehouse Secrets configuration"
    else
        # Use either DATA_WAREHOUSE_SECRET or DATA_WAREHOUSE_PLATFORM
        WAREHOUSE_TYPE="${DATA_WAREHOUSE_SECRET:-${DATA_WAREHOUSE_PLATFORM}}"
        
        # Map warehouse types to numeric values
        case "${WAREHOUSE_TYPE}" in
            "1"|"bigquery")
                echo "Configuring BigQuery secrets"
                mkdir -p /usr/src/secrets/ || { log_error "Failed to create secret directory"; exit 1; }
                
                if [ -f "/fastbi/secrets/DBT_DEPLOY_GCP_SA_SECRET" ]; then
                    echo "Reading service account secret from mounted volume"
                    read_secret "/fastbi/secrets/DBT_DEPLOY_GCP_SA_SECRET" | base64 --decode > /usr/src/secrets/sa.json || {
                        log_error "Failed to decode service account secret"
                        exit 1
                    }
                    gcloud auth activate-service-account --key-file /usr/src/secrets/sa.json || {
                        log_error "Failed to activate service account"
                        exit 1
                    }
                    export GOOGLE_APPLICATION_CREDENTIALS=/usr/src/secrets/sa.json
                    echo "GOOGLE_APPLICATION_CREDENTIALS is set!"
                else
                    log_error "Service account secret not found in mounted volume"
                    exit 1
                fi

                if [ -f "/fastbi/secrets/BIGQUERY_PROJECT_ID" ]; then
                    export BIGQUERY_PROJECT_ID=$(read_secret "/fastbi/secrets/BIGQUERY_PROJECT_ID")
                    echo "BIGQUERY_PROJECT_ID is set"
                fi

                if [ -f "/fastbi/secrets/BIGQUERY_REGION" ]; then
                    export BIGQUERY_REGION=$(read_secret "/fastbi/secrets/BIGQUERY_REGION")
                    echo "BIGQUERY_REGION is set"
                fi

                if [ -f "/fastbi/secrets/DBT_DEPLOY_GCP_SA_EMAIL" ]; then
                    export DBT_DEPLOY_GCP_SA_EMAIL=$(read_secret "/fastbi/secrets/DBT_DEPLOY_GCP_SA_EMAIL")
                    echo "DBT_DEPLOY_GCP_SA_EMAIL is set"
                fi
                ;;
            "2"|"snowflake")
                echo "Configuring Snowflake secrets"
                mkdir -p /snowsql/secrets/
                read_secret "/fastbi/secrets/SNOWFLAKE_PRIVATE_KEY" > /snowsql/secrets/rsa_key.p8 || {
                    log_error "Failed to read Snowflake private key"
                    exit 1
                }
                chmod 600 /snowsql/secrets/rsa_key.p8
                export SNOWSQL_PRIVATE_KEY_PASSPHRASE=$(read_secret "/fastbi/secrets/SNOWFLAKE_PASSPHRASE")
                echo "Snowflake secrets configured"
                ;;
            "3"|"redshift")
                echo "Configuring Redshift secrets"
                export REDSHIFT_PASSWORD=$(read_secret "/fastbi/secrets/REDSHIFT_PASSWORD")
                export REDSHIFT_USER=$(read_secret "/fastbi/secrets/REDSHIFT_USER")
                export REDSHIFT_HOST=$(read_secret "/fastbi/secrets/REDSHIFT_HOST")
                export REDSHIFT_PORT=$(read_secret "/fastbi/secrets/REDSHIFT_PORT")
                echo "Redshift secrets configured"
                ;;
            "4"|"fabric")
                echo "Configuring Fabric secrets"
                export FABRIC_USER=$(read_secret "/fastbi/secrets/FABRIC_USER")
                export FABRIC_PASSWORD=$(read_secret "/fastbi/secrets/FABRIC_PASSWORD")
                export FABRIC_SERVER=$(read_secret "/fastbi/secrets/FABRIC_SERVER")
                export FABRIC_DATABASE=$(read_secret "/fastbi/secrets/FABRIC_DATABASE")
                export FABRIC_PORT=$(read_secret "/fastbi/secrets/FABRIC_PORT")
                export FABRIC_AUTHENTICATION=$(read_secret "/fastbi/secrets/FABRIC_AUTHENTICATION")
                echo "Fabric secrets configured"
                ;;
            *)
                log_error "Invalid warehouse type value: ${WAREHOUSE_TYPE}"
                exit 1
                ;;
        esac
    fi
fi

#Authentificate to Google Platform.
if [[ -n "${SA_SECRET}" ]]; then
    echo "Authentificate at GCP."
    echo "Decrypting and saving sa.json file."
    secret_location="/usr/src/secret"
    mkdir -p ${secret_location}
    echo "${SA_SECRET}" | base64 --decode > ${secret_location}/sa.json 2>/dev/null
    if [[ -n "${SA_EMAIL}" ]]; then
        echo 'Authentificate to Google Cloud Platform with SA.'
        gcloud auth activate-service-account "${SA_EMAIL}" --key-file ${secret_location}/sa.json
    else
        echo 'Authentificate to Google Cloud Platform with SA.'
        gcloud auth activate-service-account --key-file ${secret_location}/sa.json
    fi
    if [[ -n "${PROJECT_ID}" ]]; then
        echo "The GCP Project will be set: ${PROJECT_ID}"
        gcloud config set project "${PROJECT_ID}"
        gcloud config set disable_prompts true
    else
        echo "Project Name is not in environment variables ${PROJECT_ID}."
    fi
    export GOOGLE_APPLICATION_CREDENTIALS="${secret_location}/sa.json"
fi

#Dbt-Profile.yml stored in Google Secret Manager.
if [[ -n "${DBT_PROFILE_GCP_SM_NAME}" ]]; then
    echo 'GCP Secret Manager secret name is described in environment variables.'
    echo 'Creating DBT default profile location'
    mkdir -p /root/.dbt/
    echo 'Storing profile.yml in to .dbt/profile.yml'
    gcloud secrets versions access latest --secret="${DBT_PROFILE_GCP_SM_NAME}" | base64 --decode > /root/.dbt/profiles.yml
else
    echo 'No GCP Secret Manager secret name is described in environment variables.'
fi

#Starting DBT Workload.

#Prepare the dbt project for e2e test, if it's test stage
# Check if it's the test stage
if [[ "${DATA_QUALITY_FORCE_E2E}" == "true" || "${DATA_QUALITY_FORCE_E2E}" == "True" ]]; then
    echo "re_data package is required for the dbt project for e2e test."
else
    if [[ "${TARGET}" == "test" ]]; then
        # Check if packages.yaml exists
        if [[ -f "packages.yml" ]]; then
            echo "Preparing the dbt project for e2e test, checking for re_data package in packages.yml"

            # Check if the re-data/re_data package exists in the file
            if yq . packages.yml | jq '.packages[] | select(.package == "re-data/re_data")' | grep -q .; then
                # Use yq to remove the re-data/re_data package from the file
                yq . packages.yml | jq 'del(.packages[] | select(.package == "re-data/re_data"))' | yq -y . > temp.yaml

                # Then, replace the original file with the temporary one
                mv temp.yaml packages.yml

                echo "Removed re-data/re_data package from packages.yml."
            else
                echo "re-data/re_data package not found in packages.yml. No changes made."
            fi
        fi
    fi
fi

#Downloading the dbt dependencies described in package.yml
echo 'Staying on dbt directory...'
echo 'Downloading Dbt packages.'
if [[ -n "${TARGET}" ]]; then
 dbt deps --target "${TARGET}"
else
 dbt deps
fi

echo 'Staying on dbt directory...'
if [[ "${DEBUG}" == "true" || "${DEBUG}" == "True" ]]; then
    echo "dbt debug is enabled. Testing:"
    if [[ -n "${TARGET}" ]]; then
    dbt debug --target "${TARGET}"
    else
    dbt debug
    fi
else
    echo "dbt debug is disabled."
fi

#Starting DBT SEED part. ##This part is Deprecated as we use DBT_SEED in Airflow DBT Parser. !!! Used for image testing
echo 'Staying on dbt directory...'
if [[ "${SEED}" == "true" || "${SEED}" == "True" ]]; then
    echo "Running dbt seed part."
    if [ -z "${SEED_CHECK_PERIOD}" ]; then
     SEED_CHECK_PERIOD="1 Day Ago"
     echo 'Seed check period' "${SEED_CHECK_PERIOD}"
    else
     echo 'Seed check period' "${SEED_CHECK_PERIOD}"
    fi
    seed_catalog=$(yq '."seed-paths"[]' dbt_project.yml | tr -d '"')
    printf -v execute_1 "git log -m --name-only --since='${SEED_CHECK_PERIOD}'"
    eval "${execute_1}" > /tmp/git.log
    if [ -z "$(grep -m 1 "${seed_catalog}" /tmp/git.log)" ]; then
      if [[ -n "${TARGET}" ]]; then
        if [ "${TARGET}" == "test" ]; then
            seed_update_tag="${seed_catalog}"
        else
            seed_update_tag="null"
        fi
      fi
    else
    seed_update_tag="${seed_catalog}"
    fi
    echo "dbt seed enabled."
    echo "start dbt seed procedure."
    if [ "${seed_update_tag}" == "${seed_catalog}" ]; then
        if [[ "${DBT_SEED_SHARDING}" == "true" || "${DBT_SEED_SHARDING}" == "True" ]]; then
            if [[ -n "${TARGET}" ]]; then
            echo "Run dbt seed command: dbt ${DBT_COMMAND} model ${MODEL}."
            dbt "${DBT_COMMAND}" --select "${MODEL}" --target "${TARGET}"
            else
            echo "Run dbt seed command: dbt ${DBT_COMMAND} model ${MODEL}."
            dbt "${DBT_COMMAND}" --select "${MODEL}"
            fi
        else
            if [[ -n "${TARGET}" ]]; then
            echo "Run dbt seed command: dbt seed."
            dbt seed --target "${TARGET}"
            else
            echo "Run dbt seed command: dbt seed."
            dbt seed
            fi
        fi
    echo "finishing dbt seed procedure."
    exit 0
    else
    echo "dbt seed have not changed."
    echo "finishing dbt seed procedure."
    exit 0
    fi;
fi

#Starting SNAPSHOT part. ##This part is Deprecated as we use DBT_SNAPSHOT in Airflow DBT Parser. !!! Used for image testing
echo 'Staying on dbt directory...'
if [[ "${SNAPSHOT}" == "true" || "${SNAPSHOT}" == "True" ]]; then
    echo "dbt snapshot is enabled."
    echo "start dbt snapshot procedure."
    if [[ "${SNAPSHOT_RUN_PERIOD}" == "true" || "${SNAPSHOT_RUN_PERIOD}" == "True" ]]; then
        if [[ "${DBT_SNAPSHOT_SHARDING}" == "true" || "${DBT_SNAPSHOT_SHARDING}" == "True" ]]; then
            if [[ -n "${TARGET}" ]]; then
            echo "Run dbt snapshot command: dbt ${DBT_COMMAND} model ${MODEL}."
            dbt "${DBT_COMMAND}" --select "${MODEL}" --target "${TARGET}"
            else
            echo "Run dbt snapshot command: dbt ${DBT_COMMAND} model ${MODEL}."
            dbt "${DBT_COMMAND}" --select "${MODEL}"
            fi
        else
            if [[ -n "${TARGET}" ]]; then
            echo "Run dbt snapshot command: dbt snapshot."
            dbt snapshot --target "${TARGET}"
            else
            echo "Run dbt snapshot command: dbt snapshot."
            dbt snapshot
            fi
        fi
        echo "finishing dbt snapshot procedure."
        exit 0
    else
        echo "Snapshot run period is set: ${SNAPSHOT_RUN_PERIOD}"
        exit 0
    fi
fi

#Starting Model part.
echo 'Staying on dbt directory...'

if test "${MODEL}"; then
    echo "Will run dbt model command: dbt ${DBT_COMMAND} model ${MODEL}."
    if [ "${DBT_COMMAND}" == "re_data" ]; then
        if [[ -n "${TARGET}" ]]; then
        dbt run --models package:re_data --target "${TARGET}"
        else
        dbt run --models package:re_data
        fi
    elif test "${DBT_COMMAND}" && test "${DBT_VAR}" ; then
        if [[ -n "${TARGET}" ]]; then
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt model: ${MODEL} dbt target: ${TARGET} dbt vars: ${DBT_VAR}."
            if [[ "${TARGET}" == "test" ]]; then
                printf -v execute "dbt %s --select %s --exclude package:re_data --target %s --vars '%s'" "$DBT_COMMAND" "$MODEL" "$TARGET" "$DBT_VAR"
            else
                printf -v execute "dbt %s --select %s --target %s --vars '%s'" "$DBT_COMMAND" "$MODEL" "$TARGET" "$DBT_VAR"
            fi
            eval "$execute"
        else
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt model: ${MODEL} dbt vars: ${DBT_VAR}."
            execute="dbt ${DBT_COMMAND} --select ${MODEL} --vars '${DBT_VAR}'"
            printf -v execute "dbt %s --select %s --vars '%s'" "${DBT_COMMAND}" "${MODEL}" "${DBT_VAR}"
            eval "$execute"
        fi
    elif test "${DBT_COMMAND}"; then
        if [[ -n "${TARGET}" ]]; then
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt model: ${MODEL} dbt target: ${TARGET}."
            if [[ "${TARGET}" == "test" ]]; then
                printf -v execute "dbt %s --select %s --exclude package:re_data --target %s" "$DBT_COMMAND" "$MODEL" "$TARGET"
            else
                printf -v execute "dbt %s --select %s --target %s" "$DBT_COMMAND" "$MODEL" "$TARGET"
            fi
            eval "$execute"
        else
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt model: ${MODEL}."
            execute="dbt ${DBT_COMMAND} --select ${MODEL}"
            printf -v execute "dbt %s --select %s" "${DBT_COMMAND}" "${MODEL}"
            eval "$execute"
        fi
    else
        echo "DBT COMMAND variable has not been described!"
        echo "Error - 101"
        exit 1
    fi
else
    echo "No dbt model/test name was described. Will run only command dbt ${DBT_COMMAND}"
    if test "${DBT_COMMAND}" && test "${DBT_VAR}" ; then
        if [[ -n "${TARGET}" ]]; then
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt target: ${TARGET} dbt vars: ${DBT_VAR}."
            if [[ "${TARGET}" == "test" ]]; then
                printf -v execute "dbt %s --exclude package:re_data --target %s --vars '%s'" "$DBT_COMMAND" "$TARGET" "$DBT_VAR"
            else
                printf -v execute "dbt %s --target %s --vars '%s'" "$DBT_COMMAND" "$TARGET" "$DBT_VAR"
            fi
            eval "$execute"
        else
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt vars: ${DBT_VAR}."
            execute="dbt ${DBT_COMMAND} --vars '${DBT_VAR}'"
            printf -v execute "dbt %s --vars '%s'" "${DBT_COMMAND}" "${DBT_VAR}"
            eval "$execute"
        fi
    elif test "${DBT_COMMAND}"; then
        if [[ -n "${TARGET}" ]]; then
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND} dbt target: ${TARGET}."
            if [[ "${TARGET}" == "test" ]]; then
                printf -v execute "dbt %s --exclude package:re_data --target %s" "$DBT_COMMAND" "$TARGET"
            else
                printf -v execute "dbt %s --target %s" "$DBT_COMMAND" "$TARGET"
            fi
            eval "$execute"
        else
            echo "Running dbt command with variables - dbt command: ${DBT_COMMAND}"
            execute="dbt ${DBT_COMMAND}"
            printf -v execute "dbt %s" "${DBT_COMMAND}"
            eval "$execute"
        fi
    else
        echo "DBT COMMAND variable has not been described!"
        echo "Error - 101"
        exit 1
    fi
fi

# Check if TARGET is not equal to "test"
if [[ "${TARGET}" != "test" ]]; then
    # If TARGET is not "test" and DBT_COMMAND is "test", proceed with Datahub logic
    if [ "${DBT_COMMAND}" == "test" ]; then
        # Check if DATAHUB_ENABLED is true (case insensitive)
        if [[ "${DATAHUB_ENABLED}" == "true" || "${DATAHUB_ENABLED}" == "True" ]]; then
            echo 'dbt test Metadata ingestion enabled for DataHub, preparing...'

            # Get the default project profile (Python yq syntax)
            PROJECT_PROFILE_NAME=$(yq '.profile' dbt_project.yml | tr -d '"')
            if [[ -z "$PROJECT_PROFILE_NAME" ]]; then
                echo "Error: Could not determine project profile from dbt_project.yml"
                exit 1
            fi

            # Get the default target from profiles.yml (Python yq syntax)
            DEFAULT_TARGET=$(yq ".${PROJECT_PROFILE_NAME}.target" profiles.yml | tr -d '"')
            if [[ -z "$DEFAULT_TARGET" ]]; then
                echo "Error: Could not determine default target from profiles.yml"
                exit 1
            fi
            
            # Get the warehouse type for the default target (Python yq syntax)
            WAREHOUSE_TYPE=$(yq ".${PROJECT_PROFILE_NAME}.outputs.${DEFAULT_TARGET}.type" profiles.yml | tr -d '"')
            if [[ -z "$WAREHOUSE_TYPE" ]]; then
                echo "Error: Could not determine warehouse type from profiles.yml"
                exit 1
            fi
            
            echo "Detected warehouse type: ${WAREHOUSE_TYPE}"
            
            # Set the path to the appropriate metadata collector
            METADATA_COLLECTOR_PATH="/usr/app/dbt/metadata_cli/${WAREHOUSE_TYPE}/datahub-metadata-collector.dhub.yml"
            
            if [[ ! -f "$METADATA_COLLECTOR_PATH" ]]; then
                echo "Error: Metadata collector configuration not found for ${WAREHOUSE_TYPE} at ${METADATA_COLLECTOR_PATH}"
                exit 1
            fi

            echo "Generating dbt documentation..."
            dbt docs generate --no-compile || {
                echo "Error: Failed to generate dbt documentation"
                exit 1
            }
            
            echo "Sending metadata to DataHub..."
            if ! datahub ingest -c "$METADATA_COLLECTOR_PATH" > /dev/null 2>&1; then
                echo "Warning: DataHub ingestion completed with errors"
            fi
            
            echo "Metadata ingestion completed."
        fi
    fi
fi

#Additional Logs for Debug Mode.
echo 'Staying on dbt directory...'
if [[ "${MODEL_DEBUG_LOG}" == "true" || "${MODEL_DEBUG_LOG}" == "True" ]]; then
    echo "Debug is enabled, showing output from DBT Project Logs Catalog."
    cat /data/dbt/"${DBT_PROJECT_DIRECTORY}"/logs/dbt.log
else
    echo "Model debug logs not enabled!"
fi

# Check the log file for errors
if [ -s "$LOG_FILE" ]; then
  log_error "Errors occurred during dbt task execution."
  log_error "$(cat "${LOG_FILE}")"
  exit 1
else
  echo "dbt task executed successfully."
fi

exec "$@"
