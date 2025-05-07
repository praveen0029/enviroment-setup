# Databricks Environment Setup
<br>A tool to extract configuration details from a Databricks environment and recreate similar environments in different deployment stages.</br>
## Overview
<br>This utility scrapes the complete configuration of a Databricks environment (e.g., development) and provides the necessary information to recreate an identical environment in another stage (staging or production). It helps ensure consistency across different environments and simplifies the migration process.</br>
## Features

Complete Environment Scraping: Extracts all relevant details from a source Databricks environment\
Cross-Environment Deployment: Facilitates recreating environments across dev, staging, and production\
Configuration Preservation: Maintains all important settings during migration\
Automated Setup: Reduces manual configuration errors when creating new environments

## Prerequisites

Python 3.7+\
Databricks CLI configured with appropriate access tokens\
Required Python packages (see requirements.txt)\
Appropriate permissions to access source and target Databricks workspaces

## Components Scraped
The tool extracts information about the following Databricks components:\

**Clusters**: Configurations, sizes, libraries, and auto-scaling settings\
**Jobs**: Schedules, parameters, cluster attachments\
**Notebooks**: Content, metadata, permissions\
**Libraries**: All installed libraries and their versions\
**Workspace**: Structure, folders, and permissions\
**Secrets**: Secret scopes (not the secret values themselves)\
**DBFS Files**: Critical files stored in DBFS\
**Tables**: Table schemas and properties\
**Users/Groups**: Permission settings (optional)
