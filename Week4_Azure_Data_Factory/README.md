# Week 4 - Azure Cloud Fundamentals & Azure Data Factory

## Objective
Build an Azure Data Factory pipeline to read a CSV file from Azure Blob Storage, validate its metadata, and copy it to another location.

## Services Used
- Azure Resource Group
- Azure Storage Account
- Azure Blob Storage
- Azure Data Factory
- Linked Service
- Source Dataset
- Destination Dataset
- Get Metadata Activity
- Copy Data Activity
- IAM Roles

## Pipeline Workflow

Blob Storage (CSV)
        ↓
   Get Metadata
        ↓
    Copy Data
        ↓
Destination Blob Storage

## Output
- Pipeline executed successfully.
- Metadata validated successfully.
- CSV file copied successfully.
- IAM roles assigned successfully.

## Repository Contents

- README.md
- Screenshots.pdf (contains all required screenshots)

## Summary

In this assignment, Azure Blob Storage was configured as the data source and destination. A Linked Service and datasets were created in Azure Data Factory. A pipeline consisting of Get Metadata and Copy Data activities was developed to validate the source file and copy it to the destination. The pipeline executed successfully, and the required IAM roles were assigned.