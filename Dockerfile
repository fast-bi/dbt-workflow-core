# syntax=docker/dockerfile:1
##
#  dbt-workflow-core
#  One image for running dbt (dbt-core 1.x), code-quality checks
#  (dbtf lint / dbtf format, dbt-bouncer, yamllint) and DataHub ingestion.
#  Python package versions are pinned in requirements.txt.
#
#  `dbt`  = dbt-core 1.x (pip) - runs, tests and parses the projects.
#  `dbtf` = dbt v2 binary      - used ONLY for `dbtf lint` and `dbtf format`,
#           which need no warehouse connection. Projects are not migrated.
#  Target platform is chosen at build time: docker buildx build --platform ...
##
ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim-bookworm
LABEL maintainer="support@fast.bi" \
      org.opencontainers.image.title="dbt-workflow-core" \
      org.opencontainers.image.vendor="fast.bi" \
      org.opencontainers.image.source="https://github.com/fast-bi/dbt-workflow-core"

ENV PYTHONIOENCODING=utf-8 \
    LANG=C.UTF-8 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS=false

# System packages + Google Cloud CLI (gcloud, bq, gsutil) in a single layer
RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    gnupg \
    jq \
    make \
    openssh-client \
  && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    > /etc/apt/sources.list.d/google-cloud-sdk.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends google-cloud-cli \
  && rm -f /usr/lib/google-cloud-sdk/bin/anthoscli \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Python packages: one resolve, pinned versions, fail the build on conflicts
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
  && python -m pip install -r /tmp/requirements.txt \
  && python -m pip check \
  && rm -f /tmp/requirements.txt \
  && dbt --version \
  && dbt-bouncer --version \
  && yamllint --version \
  && datahub version

# dbt v2 binary for linting/formatting only, exposed as `dbtf` so it never
# shadows dbt-core's `dbt`
ARG DBT_V2_VERSION=2.0.12
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh \
    | sh -s -- --version "${DBT_V2_VERSION}" --prefix /opt/dbt-v2 \
  && ln -s /opt/dbt-v2/bin/dbt /usr/local/bin/dbtf \
  && dbtf --version \
  && dbt --version | grep -q "installed: 1\."

WORKDIR /usr/app/dbt/

# Application files
COPY macros/ /usr/app/dbt/macros/
COPY metadata_cli/ /usr/app/dbt/metadata_cli/
COPY --chmod=755 api-entrypoint.sh dbt_bq_dataset_label_add.sh /usr/app/dbt/
COPY --chmod=755 dbt-refresh-incremental/model_incremental_refresh.sh /usr/app/dbt/

ENTRYPOINT ["/bin/bash", "-c", "/usr/app/dbt/api-entrypoint.sh"]
