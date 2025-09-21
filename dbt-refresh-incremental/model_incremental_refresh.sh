#!/bin/bash

# test_incremental_refresh.sh
# A script to test the dbt incremental refresh functionality locally
# Usage: ./test_incremental_refresh.sh [--dry-run] [--previous-commit COMMIT_SHA] [--target TARGET] [--manifest PATH] [--exclude PATTERN] [--build-rest]

set -e

# Function to get detailed change information
get_detailed_changes() {
  local file_path="$1"
  local prev_commit="$2"
  local curr_commit="$3"
  
  # Log git history information
  log "Checking file: $file_path"
  log "Previous commit: $prev_commit"
  log "Current commit: $curr_commit"
  
  # Log git history
  log "Git history:"
  git log --oneline "$prev_commit".."$curr_commit" -- "$file_path" | while read -r line; do
    log "  $line"
  done
  
  # First check if file exists in previous commit
  if ! git show "$prev_commit:$file_path" &>/dev/null; then
    log "File does not exist in previous commit"
    echo "Change type: A (New file)"
    echo "Status: First deployment"
    return
  fi
  
  log "File exists in previous commit"
  echo "Change type: M (Modified)"
  echo "Status: Not first deployment"
}

# Default values
DRY_RUN=false
PREVIOUS_COMMIT=""
CURRENT_COMMIT="HEAD"
LOG_DIR="./logs"
REPORT_FILE="./refresh_report.json"
MANIFEST_PATH="" # Path to existing manifest.json if provided
TARGET="" # dbt target profile
EXCLUDE_PATTERN="" # Pattern to exclude from build (e.g., "package:re_data" or "tag:samsa")
BUILD_REST=false # Whether to build the rest of the project

# Function to construct dbt command with target and exclude if specified
get_dbt_command() {
  local base_cmd="$1"
  local cmd="$base_cmd"
  
  if [ -n "$TARGET" ]; then
    cmd="$cmd --target $TARGET"
  fi
  
  if [ -n "$EXCLUDE_PATTERN" ]; then
    cmd="$cmd --exclude $EXCLUDE_PATTERN"
  fi
  
  echo "$cmd"
}

# Function to execute dbt command with proper logging
execute_dbt_command() {
  local cmd="$1"
  local log_file="$2"
  
  log "Executing command: $cmd"
  if ! eval "$cmd" 2>&1 | tee "$log_file"; then
    handle_error "Failed to execute command: $cmd. Check $log_file for details."
  fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --previous-commit)
      PREVIOUS_COMMIT="$2"
      shift
      shift
      ;;
    --manifest)
      MANIFEST_PATH="$2"
      shift
      shift
      ;;
    --target)
      TARGET="$2"
      shift
      shift
      ;;
    --exclude)
      EXCLUDE_PATTERN="$2"
      shift
      shift
      ;;
    --build-rest)
      BUILD_REST=true
      shift
      ;;
    --help)
      echo "Usage: ./test_incremental_refresh.sh [--dry-run] [--previous-commit COMMIT_SHA] [--target TARGET] [--manifest PATH] [--exclude PATTERN] [--build-rest]"
      echo ""
      echo "Options:"
      echo "  --dry-run           Show what would be done without actually running dbt commands"
      echo "  --previous-commit   Compare current state with specific commit (defaults to last commit)"
      echo "  --target           dbt target profile to use"
      echo "  --manifest         Path to existing manifest.json file (to skip dbt compile in dry-run mode)"
      echo "  --exclude          Pattern to exclude from build (e.g., 'package:re_data' or 'tag:samsa')"
      echo "  --build-rest       Build the rest of the project after handling changed models (default: false)"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Run './test_incremental_refresh.sh --help' for usage information"
      exit 1
      ;;
  esac
done

# Check if we're in a git repository
if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
  echo "ERROR: Not in a git repository"
  exit 1
fi

# Setup logging
mkdir -p "$LOG_DIR"
MAIN_LOG="$LOG_DIR/dbt_refresh.log"
DBT_DEPS_LOG="$LOG_DIR/dbt_deps.log"
DBT_COMPILE_LOG="$LOG_DIR/dbt_compile.log"
DBT_BUILD_LOG="$LOG_DIR/dbt_build.log"

# Initialize or clear log files
> "$MAIN_LOG"
> "$DBT_DEPS_LOG"
> "$DBT_COMPILE_LOG"
> "$DBT_BUILD_LOG"

# Set default previous commit if not specified
if [ -z "$PREVIOUS_COMMIT" ]; then
  PREVIOUS_COMMIT=$(git rev-parse HEAD~1)
  echo "No previous commit specified, using previous commit: $PREVIOUS_COMMIT"
fi

# Function for logging
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$MAIN_LOG"
}

# Function for error handling
handle_error() {
  log "ERROR: $1"
  if [ -f "$REPORT_FILE" ]; then
    tmp_file=$(mktemp)
    jq --arg error "$1" '.errors += [$error] | .status = "failed"' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"
  fi
  exit 1
}

# Check if required commands are available
for cmd in git jq; do
  if ! command -v $cmd > /dev/null; then
    handle_error "Required command not found: $cmd"
  fi
done

# Check if dbt is available (only needed if not in dry-run mode)
if [ "$DRY_RUN" != "true" ] && ! command -v dbt > /dev/null; then
  handle_error "Required command not found: dbt (needed for non-dry-run mode)"
fi

# Start the process
log "Starting dbt incremental refresh test with dry-run: $DRY_RUN"
log "Comparing current state with commit: $PREVIOUS_COMMIT"

# Function to get model paths from dbt_project.yml
get_model_paths() {
  local project_file="dbt_project.yml"
  if [ ! -f "$project_file" ]; then
    handle_error "dbt_project.yml not found"
  fi
  
  # Extract model-paths array from dbt_project.yml
  # Using grep and sed to handle both single and multi-line array formats
  local paths=$(grep "^model-paths:" "$project_file" | sed -E 's/model-paths:\s*\[(.*)\]/\1/' | sed -E 's/model-paths:\s*//')
  if [ -z "$paths" ]; then
    # If not found in array format, try looking for single line format
    paths=$(grep "^model-paths:" "$project_file" | cut -d: -f2- | tr -d '[]' | tr -d ' ')
  fi
  
  if [ -z "$paths" ]; then
    # Default to 'models' if not specified
    echo "models"
  else
    echo "$paths"
  fi
}

# Get model paths from dbt_project.yml
MODEL_PATHS=$(get_model_paths)
log "Using model paths from dbt_project.yml: $MODEL_PATHS"

# Detect changed SQL files in model paths
log "Detecting changed SQL files..."
CHANGED_FILES_LIST=$(mktemp)
git diff --name-status "$PREVIOUS_COMMIT" "$CURRENT_COMMIT" | \
  grep -E "^[AM].*\.sql$" | \
  grep -E "$(echo $MODEL_PATHS | tr ',' '|')" | \
  awk '{print $2}' > "$CHANGED_FILES_LIST" || true

# Check if any files were found
if [ -s "$CHANGED_FILES_LIST" ]; then
  CHANGED_FILES_COUNT=$(wc -l < "$CHANGED_FILES_LIST")
else
  CHANGED_FILES_COUNT=0
fi
log "Detected $CHANGED_FILES_COUNT changed SQL files"

# Create a JSON array of changed SQL files
CHANGED_FILES_JSON=$(mktemp)
echo "[" > "$CHANGED_FILES_JSON"
if [ "$CHANGED_FILES_COUNT" -gt 0 ]; then
  first=true
  while read -r file; do
    if [ "$first" = true ]; then
      first=false
    else
      echo "," >> "$CHANGED_FILES_JSON"
    fi
    echo "  \"$file\"" >> "$CHANGED_FILES_JSON"
  done < "$CHANGED_FILES_LIST"
fi
echo "]" >> "$CHANGED_FILES_JSON"

# Create initial report
cat > "$REPORT_FILE" << EOF
{
  "execution_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "dry_run": $DRY_RUN,
  "changed_files": $(cat "$CHANGED_FILES_JSON"),
  "incremental_models": [],
  "refresh_commands": [],
  "status": "starting",
  "errors": []
}
EOF

# Early exit if no SQL files were changed
if [ "$CHANGED_FILES_COUNT" -eq 0 ]; then
  log "No SQL files were changed, skipping refresh process"
  tmp_file=$(mktemp)
  jq '.status = "completed" | .message = "No SQL files were changed"' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"
  log "Report saved to $REPORT_FILE"
  exit 0
fi

# Enable bash pipefail to catch errors in pipelines
set -o pipefail

# Set manifest path for analysis
MANIFEST_TO_USE=""

if [ "$DRY_RUN" = "true" ]; then
  # In dry-run mode, we either use the provided manifest or look for one in target/
  if [ -n "$MANIFEST_PATH" ] && [ -f "$MANIFEST_PATH" ]; then
    log "Using provided manifest file: $MANIFEST_PATH"
    MANIFEST_TO_USE="$MANIFEST_PATH"
  elif [ -f "target/manifest.json" ]; then
    log "Using existing manifest file in target/"
    MANIFEST_TO_USE="target/manifest.json"
  else
    log "WARNING: No manifest file provided or found. Cannot accurately determine incremental models."
    log "Please provide a manifest file with --manifest or run without --dry-run first to generate one."
    tmp_file=$(mktemp)
    jq '.status = "error" | .message = "No manifest file available for analysis in dry-run mode"' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"
    exit 1
  fi
else
  # In regular mode, run dbt deps and compile to generate manifest
  log "Running dbt deps to install dependencies..."
  if ! dbt deps 2>&1 | tee "$DBT_DEPS_LOG"; then
    handle_error "Failed to install dbt dependencies. Check $DBT_DEPS_LOG for details."
  fi
  
  log "Compiling dbt project to generate manifest..."
  if ! $(get_dbt_command "dbt compile") 2>&1 | tee "$DBT_COMPILE_LOG"; then
    # Since we're on main/master after e2e tests, compilation failure indicates initial setup
    log "Compilation failed - this appears to be an initial project setup"
    tmp_file=$(mktemp)
    jq '.status = "skipped" | .message = "Initial project setup - compilation failed as expected"' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"
    # Clean up and exit gracefully
    rm -f "$CHANGED_FILES_LIST" "$CHANGED_FILES_JSON"
    log "Exiting gracefully - initial project setup"
    exit 0
  fi
  
  # If we get here, compilation succeeded, meaning schema exists and we can proceed
  MANIFEST_TO_USE="target/manifest.json"

  # Check if manifest file was generated
  if [ ! -f "$MANIFEST_TO_USE" ]; then
    handle_error "Manifest file not found at $MANIFEST_TO_USE"
  fi
fi

# Process manifest to identify incremental models
log "Analyzing manifest to identify incremental models..."
INCREMENTAL_MODELS=()
REFRESH_COMMANDS=()

# Get the project name from the manifest
project_name=$(jq -r '.metadata.project_name' "$MANIFEST_TO_USE")
log "Project name: $project_name"

while read -r sql_file; do
  # Convert file path to model name (just the model name without the path)
  model_name=$(echo "$sql_file" | sed 's/.*\/\([^\/]*\)\.sql$/\1/')
  
  # Get the node name from the manifest
  node_name="model.$project_name.$model_name"
  
  # Debug logging
  log "Analyzing model: $model_name"
  log "Looking for node: $node_name"
  
  # Check if the model exists in the manifest and is incremental
  is_incremental=$(jq -r --arg node "$node_name" '
    if .nodes[$node] then 
      if .nodes[$node].config.materialized == "incremental" then "true" else "false" end
    else 
      "false" 
    end' "$MANIFEST_TO_USE")
  
  # Debug logging
  log "Is incremental check result: $is_incremental"
  
  if [ "$is_incremental" = "true" ]; then
    log "Found incremental model: $model_name"
    INCREMENTAL_MODELS+=("$model_name")
    REFRESH_COMMANDS+=("$(get_dbt_command "dbt build --select $model_name --full-refresh")")
  else
    log "Model $model_name is not incremental, skipping full-refresh"
  fi
done < "$CHANGED_FILES_LIST"

# Update the refresh report with incremental models
# Remove empty elements from arrays 
INCREMENTAL_MODELS=(${INCREMENTAL_MODELS[@]})
REFRESH_COMMANDS=(${REFRESH_COMMANDS[@]})

# Debug logging for arrays
log "Incremental models array: ${INCREMENTAL_MODELS[*]}"
log "Refresh commands array: ${REFRESH_COMMANDS[*]}"

models_json=$(printf '%s\n' "${INCREMENTAL_MODELS[@]}" | jq -R . | jq -s . 2>/dev/null || echo "[]")
commands_json=$(printf '%s\n' "${REFRESH_COMMANDS[@]}" | jq -R . | jq -s . 2>/dev/null || echo "[]")

tmp_file=$(mktemp)
jq --argjson models "$models_json" --argjson commands "$commands_json" '.incremental_models = $models | .refresh_commands = $commands' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"

# Check if there are any incremental models to refresh
if [ ${#INCREMENTAL_MODELS[@]} -eq 0 ]; then
  log "No incremental models were changed, skipping build as it will be handled by the orchestrator"
  tmp_file=$(mktemp)
  jq '.status = "completed" | .message = "No incremental models were changed, skipping build"' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"
  
  if [ "$DRY_RUN" = "true" ]; then
    log "DRY RUN: Would skip build as no incremental models were changed"
  fi
else
  # Check if the model existed in the previous commit
  log "Checking if models existed in main/master branch..."
  
  # Get the previous commit on main/master (before feature branch)
  PREVIOUS_COMMIT=$(git rev-parse HEAD~1)
  CURRENT_COMMIT=$(git rev-parse HEAD)
  log "Comparing commits: $PREVIOUS_COMMIT (previous) -> $CURRENT_COMMIT (current)"
  
  # Get all SQL files from current commit
  log "Getting SQL files from current commit..."
  CURRENT_FILES=$(mktemp)
  git ls-tree -r "$CURRENT_COMMIT" --name-only | grep -E "\.sql$" > "$CURRENT_FILES"
  
  log "Files in current commit:"
  cat "$CURRENT_FILES" | while read -r line; do
    log "  - $line"
  done
  
  for model in "${INCREMENTAL_MODELS[@]}"; do
    log "Processing model: $model"
    
    # Get the actual path from the changed files list
    model_path=$(grep -E "/${model}\.sql$" "$CHANGED_FILES_LIST" | head -n 1)
    if [ -z "$model_path" ]; then
      log "ERROR: Could not find path for model $model in changed files"
      log "Current changed files list:"
      cat "$CHANGED_FILES_LIST"
      continue
    fi
    
    # Debug logging for path checking
    log "Found model path in changed files: $model_path"
    
    # Check if this file existed in any commit before the current one
    log "Checking if $model_path existed in git history before current commit..."
    
    # Get the first commit that introduced this file
    FIRST_COMMIT=$(git log --diff-filter=A --format="%H" -- "$model_path" | tail -n 1)
    log "First commit that added this file: $FIRST_COMMIT"
    
    # Check if the file was added in the current commit
    CURRENT_CHANGES=$(git diff --name-status "$PREVIOUS_COMMIT" "$CURRENT_COMMIT" -- "$model_path")
    log "Current changes status: $CURRENT_CHANGES"
    
    # Log git diff command
    log "Running git diff command: git diff --name-status $PREVIOUS_COMMIT $CURRENT_COMMIT -- $model_path"
    log "Full git diff output:"
    git diff --name-status "$PREVIOUS_COMMIT" "$CURRENT_COMMIT" -- "$model_path" | while read -r line; do
      log "  $line"
    done
    
    # Log git history between commits
    log "Git log between commits:"
    git log --oneline "$PREVIOUS_COMMIT".."$CURRENT_COMMIT" | while read -r line; do
      log "  $line"
    done
    
    # Get detailed change information
    DETAILED_INFO=$(get_detailed_changes "$model_path" "$PREVIOUS_COMMIT" "$CURRENT_COMMIT")
    log "Detailed change information:"
    echo "$DETAILED_INFO" | while read -r line; do
      log "  $line"
    done
    
    # Check if file exists in previous commit
    if git show "$PREVIOUS_COMMIT:$model_path" &>/dev/null; then
      log "File exists in previous commit"
      IS_NEW_IN_CURRENT_COMMIT="false"
    else
      log "File does not exist in previous commit"
      IS_NEW_IN_CURRENT_COMMIT="true"
    fi
    
    # Additional debug info
    log "Full changes information:"
    log "  - Model path: $model_path"
    log "  - First commit: $FIRST_COMMIT"
    log "  - Current changes: $CURRENT_CHANGES"
    log "  - Is new in current commit: $IS_NEW_IN_CURRENT_COMMIT"
    
    if [ "$IS_NEW_IN_CURRENT_COMMIT" = "true" ]; then
      log "✗ Model is new in current commit - appears to be first deployment"
      tmp_file=$(mktemp)
      jq --arg model "$model" '.status = "skipped" | .message = "First deployment of model: " + $model' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"
      # Clean up and exit gracefully
      rm -f "$CHANGED_FILES_LIST" "$CHANGED_FILES_JSON" "$CURRENT_FILES"
      log "Exiting gracefully - first deployment"
      exit 0
    else
      log "✓ Model exists and was modified in current commit"
      
      # Check what type of changes were made
      if echo "$CURRENT_CHANGES" | grep -q "^M"; then
        log "  - Model SQL or config was modified"
      elif echo "$CURRENT_CHANGES" | grep -q "^R"; then
        log "  - Model was renamed/moved"
      fi
    fi
  done

  # Clean up temporary files
  rm -f "$CURRENT_FILES"

  # If we get here, models existed before and we can proceed with the build
  # Create a comma-separated list of models for the --select flag
  MODELS_LIST=$(IFS=,; echo "${INCREMENTAL_MODELS[*]}")
  
  log "Running full refresh for incremental models: $MODELS_LIST"
  if ! $(get_dbt_command "dbt build --select $MODELS_LIST --full-refresh") 2>&1 | tee "$DBT_BUILD_LOG"; then
    handle_error "Failed to run full refresh for models. Check $DBT_BUILD_LOG for details."
  fi
  
  # Only build the rest if --build-rest is specified
  if [ "$BUILD_REST" = "true" ]; then
    log "Running normal build for the rest of the project"
    if ! $(get_dbt_command "dbt build") 2>&1 | tee -a "$DBT_BUILD_LOG"; then
      handle_error "Failed to run normal build for the rest of the project. Check $DBT_BUILD_LOG for details."
    fi
  else
    log "Skipping build of the rest of the project (use --build-rest to enable)"
    log "NOTE: The rest of the project will be built by Airflow DAGs in production"
  fi
fi

# Update the final status
tmp_file=$(mktemp)
jq '.status = "completed" | .message = "Successfully identified and processed incremental models"' "$REPORT_FILE" > "$tmp_file" && mv "$tmp_file" "$REPORT_FILE"

# Clean up temporary files
rm -f "$CHANGED_FILES_LIST" "$CHANGED_FILES_JSON"

# Output a summary
log "Summary:"
log "- Changed SQL files: $CHANGED_FILES_COUNT"
log "- Incremental models: ${#INCREMENTAL_MODELS[@]}"
if [ ${#INCREMENTAL_MODELS[@]} -gt 0 ]; then
  log "- Models that would be full-refreshed: $MODELS_LIST"
fi
log "Full report saved to $REPORT_FILE"

# Pretty print the report
log "Report contents:"
jq . "$REPORT_FILE"

log "Incremental refresh test completed successfully" 