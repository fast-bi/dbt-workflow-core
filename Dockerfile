##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
FROM --platform=$build_for python:3.11.11-slim-bullseye as base
LABEL maintainer=support@fast.bi

# System setup
RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
    curl \
    apt-transport-https \
    gnupg \
    cl-base64 \
    jq
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN apt update && apt install google-cloud-sdk -y
RUN apt-get update && apt-get upgrade -y
RUN apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python packages
RUN python -m pip install --upgrade pip setuptools wheel yq pytz pandas colorama --no-cache-dir
RUN python -m pip install --upgrade acryl-datahub
RUN python -m pip install --upgrade 'acryl-datahub[dbt]'
RUN python -m pip install --upgrade 'acryl-datahub[datahub-rest]'
RUN python -m pip install --no-cache-dir dbt-bigquery==1.9.2
RUN python -m pip install --no-cache-dir dbt-snowflake==1.9.4
RUN python -m pip install --no-cache-dir dbt-redshift==1.9.5
RUN python -m pip install --no-cache-dir dbt-fabric==1.9.6
RUN python -m pip install --upgrade dbt-coverage==0.3.9

# Set docker basics
WORKDIR /usr/app/dbt/
LABEL maintainer=TeraSky(c)

# Copy application files
COPY ./macros/*.sql /usr/app/dbt/macros/
COPY ./api-entrypoint.sh /usr/app/dbt/
#DEPRECATED COPY ./cleanup_e2e_test.sh /usr/app/dbt/
COPY ./dbt_bq_dataset_label_add.sh /usr/app/dbt/
COPY ./dbt_lint/*.py /usr/app/dbt/dbt_lint/
COPY ./dbt-refresh-incremental/model_incremental_refresh.sh /usr/app/dbt/
RUN mkdir -p /usr/app/dbt/metadata_cli/{bigquery,redshift,snowflake}
COPY ./metadata_cli/ /usr/app/dbt/metadata_cli/

# Set permissions
RUN chmod 755 /usr/app/dbt/api-entrypoint.sh
#DEPRECATED RUN chmod 755 /usr/app/dbt/cleanup_e2e_test.sh
RUN chmod 755 /usr/app/dbt/dbt_bq_dataset_label_add.sh
RUN chmod 755 /usr/app/dbt/dbt_lint/*.py
RUN chmod 755 /usr/app/dbt/model_incremental_refresh.sh

ENTRYPOINT ["/bin/bash", "-c", "/usr/app/dbt/api-entrypoint.sh" ]