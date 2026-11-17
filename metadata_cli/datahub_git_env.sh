#!/bin/bash
# Derives the DBT_GIT_* variables used by datahub-catalog-collector.dhub.yml.
# Source this file from the dbt project root:  source datahub_git_env.sh
#
# Inputs (first non-empty value wins):
#   repo URL : DBT_GIT_REPO_URL, GIT_REPO_URL, GIT_URL
#   branch   : DBT_GIT_BRANCH, GIT_BRANCH_MAIN, GIT_BRANCH, "main"
#   subdir   : DBT_GIT_URL_SUBDIR, DBT_PROJECT_DIRECTORY, DBT_PROJECT_NAME
# The repo URL is normalised to "https://host/org/repo": credentials, a
# trailing ".git" or "/" are removed and ssh URLs are converted to https.

_dbt_git_repo="${DBT_GIT_REPO_URL:-${GIT_REPO_URL:-${GIT_URL:-}}}"
# git@host:org/repo(.git)  ->  https://host/org/repo
if [[ "${_dbt_git_repo}" =~ ^(ssh://)?git@([^:/]+)[:/](.+)$ ]]; then
    _dbt_git_repo="https://${BASH_REMATCH[2]}/${BASH_REMATCH[3]}"
fi
# https://user:token@host/...  ->  https://host/...
if [[ "${_dbt_git_repo}" =~ ^(https?://)[^/@]+@(.+)$ ]]; then
    _dbt_git_repo="${BASH_REMATCH[1]}${BASH_REMATCH[2]}"
fi
_dbt_git_repo="${_dbt_git_repo%/}"
_dbt_git_repo="${_dbt_git_repo%.git}"

_dbt_git_subdir="${DBT_GIT_URL_SUBDIR:-${DBT_PROJECT_DIRECTORY:-${DBT_PROJECT_NAME:-}}}"
_dbt_git_subdir="${_dbt_git_subdir#/}"
_dbt_git_subdir="${_dbt_git_subdir%/}"

export DBT_GIT_REPO_URL="${_dbt_git_repo}"
export DBT_GIT_BRANCH="${DBT_GIT_BRANCH:-${GIT_BRANCH_MAIN:-${GIT_BRANCH:-main}}}"
export DBT_GIT_URL_SUBDIR="${_dbt_git_subdir}"
unset _dbt_git_repo _dbt_git_subdir

if [[ -z "${DBT_GIT_REPO_URL}" ]]; then
    echo "datahub_git_env: no repository URL found (set GIT_REPO_URL)" >&2
    return 1 2>/dev/null || exit 1
fi
echo "datahub_git_env: repo=${DBT_GIT_REPO_URL} branch=${DBT_GIT_BRANCH} subdir=${DBT_GIT_URL_SUBDIR:-<repository root>}"
