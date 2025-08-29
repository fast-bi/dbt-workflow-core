# DBT Workflow Core

A Kubernetes operator image for executing dbt workloads and CI/CD metadata operations, supporting multiple data warehouse platforms with comprehensive data transformation and metadata management capabilities.

## Overview

This Docker image provides a complete dbt execution environment for Kubernetes-based data workflows and CI/CD pipelines. It's designed to be used as a Kubernetes operator for running dbt workloads efficiently, while also supporting metadata operations and sharing across different Fast.BI services. The image includes support for multiple data warehouse platforms, automated secret management, and comprehensive error handling for production-grade data transformation workflows.

## Architecture

### Core Components

**DBT Execution Engine**: Provides a complete dbt runtime environment with support for multiple data warehouse adapters including BigQuery, Snowflake, Redshift, and Fabric.

**Kubernetes Operator**: Designed to run as a Kubernetes pod with proper resource management, secret handling, and lifecycle management for dbt workloads.

**Metadata Management System**: Integrates with DataHub for metadata collection and sharing across Fast.BI services, enabling comprehensive data lineage tracking.

**Secret Management**: Handles secure authentication for multiple data warehouse platforms through Kubernetes secrets and service account management.

**Git Integration**: Supports repository cloning, branch management, and project configuration for CI/CD workflows.

## Docker Image

### Base Image
- **Base**: Python 3.11.11-slim-bullseye

### Build

```bash
# Build the image
./build.sh

# Or manually
docker build -t dbt-workflow-core .
```

### Build Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `build_for` | Target platform for the build | `linux/amd64` |

### Environment Variables

The container expects the following environment variables:

- `GITLINK_SECRET` - Git repository URL for dbt project cloning
- `GIT_BRANCH` - Git branch to clone (optional, defaults to main)
- `DBT_PROJECT_DIRECTORY` - Directory within the repository containing the dbt project
- `PROFILES_DIR_PATH` - Path to dbt profiles (default, repo, or custom)
- `DATA_WAREHOUSE_PLATFORM` - Target data warehouse platform (bigquery, snowflake, redshift, fabric)
- `DBT_COMMAND` - dbt command to execute (run, test, seed, etc.)
- `MODEL` - Specific model or selection criteria for dbt execution
- `GIT_URL` - Git repository URL for DNS resolution testing

### Configuration Files

The image supports configuration through the following files:

- `macros/*.sql`: Custom dbt macros for data transformation logic
- `dbt_lint/*.py`: Python scripts for dbt project linting and validation
- `metadata_cli/`: DataHub metadata collection configurations
- `dbt-refresh-incremental/`: Incremental model refresh utilities

## Main Functionality

### DBT Workflow Execution

The image orchestrates a complete dbt workflow execution process:

1. **Environment Setup**: Configures data warehouse authentication and project environment
2. **Repository Cloning**: Clones dbt project from Git repository with branch support
3. **Project Configuration**: Sets up dbt profiles and project structure
4. **Secret Management**: Handles authentication for multiple data warehouse platforms
5. **DBT Execution**: Runs specified dbt commands with proper error handling
6. **Metadata Collection**: Collects and shares metadata with DataHub for lineage tracking

### Supported Data Warehouse Platforms

**BigQuery**: Full support with GCP service account authentication and dataset management
**Snowflake**: Complete dbt integration with Snowflake-specific optimizations
**Redshift**: AWS Redshift support with proper connection management
**Fabric**: Microsoft Fabric data warehouse integration

### CI/CD Integration Features

- **Automated Repository Management**: Git cloning with branch support and DNS resolution
- **Secret Injection**: Kubernetes secret management for secure authentication
- **Error Handling**: Comprehensive error logging and handling for production workflows
- **Metadata Sharing**: Integration with DataHub for cross-service metadata management
- **Resource Cleanup**: Proper cleanup of temporary files and authentication credentials

### Error Handling

- Comprehensive error logging to dedicated log files
- Graceful handling of authentication failures
- DNS resolution testing for Git repositories
- Proper cleanup of temporary resources and secrets
- Detailed error reporting for troubleshooting

### Maintenance Tasks

- **Workflow Health Monitoring**: Monitors dbt execution status and performance
- **Resource Management**: Manages Kubernetes resources and memory usage
- **Secret Rotation**: Supports service account key rotation and credential updates
- **Metadata Synchronization**: Ensures metadata consistency across Fast.BI services

## Testing

### Health Checks

The image includes built-in health checks:

```bash
# Check container health
docker inspect --format='{{.State.Health.Status}}' dbt-workflow-core

# View health check logs
docker inspect --format='{{range .State.Health.Log}}{{.Output}}{{end}}' dbt-workflow-core
```

### DBT Command Examples

```bash
# Run dbt models
docker run dbt-workflow-core dbt run --select model_name

# Run dbt tests
docker run dbt-workflow-core dbt test --select model_name

# Run dbt seeds
docker run dbt-workflow-core dbt seed --select seed_name

# Run dbt docs
docker run dbt-workflow-core dbt docs generate
```

## Troubleshooting

### Common Issues

#### Issue: Git Repository Cloning Failure
**Problem**: Cannot clone dbt project from Git repository

**Solution**: Verify GITLINK_SECRET is correct and repository is accessible

#### Issue: Data Warehouse Authentication Failure
**Problem**: Failed to authenticate with data warehouse platform

**Solution**: Check mounted secrets and service account configuration

#### Issue: DBT Project Configuration Error
**Problem**: dbt project structure or profiles configuration is invalid

**Solution**: Verify DBT_PROJECT_DIRECTORY and PROFILES_DIR_PATH settings

#### Issue: DNS Resolution Failure
**Problem**: Cannot resolve Git repository URL

**Solution**: Check network connectivity and DNS configuration

#### Issue: Metadata Collection Failure
**Problem**: Failed to collect or share metadata with DataHub

**Solution**: Verify DataHub connectivity and metadata configuration

### Getting Help

- **Documentation**: [Fast.BI Documentation](https://wiki.fast.bi)
- **Issues**: [GitHub Issues](https://github.com/fast-bi/dbt-workflow-core/issues)
- **Email**: support@fast.bi

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

```
MIT License

Copyright (c) 2025 Fast.BI

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

This project is part of the FastBI platform infrastructure.

## Support and Maintain by Fast.BI

For support and questions, contact: support@fast.bi
