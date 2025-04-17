# Scripts

This directory contains utility scripts for the Dataspot project.

## Organization Hierarchy Builder

The `build_organization_hierarchy.py` script builds the organization structure in Dataspot's DNK scheme based on data from the ODS API. It uses the bulk upload approach for efficiency.

### Usage

```
python scripts/build_organization_hierarchy.py [options]
```

#### Options

- `--validate-urls`: Validate Staatskalender URLs (can be slow)
- `--max-batches NUMBER`: Maximum number of batches to retrieve (default: all)
- `--batch-size NUMBER`: Number of records to retrieve in each API call (default: 100)
- `--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}`: Set the logging level (default: INFO)

### Examples

Basic usage:
```
python scripts/build_organization_hierarchy.py
```

Limit to 5 batches (for testing):
```
python scripts/build_organization_hierarchy.py --max-batches 5
```

Enable URL validation and verbose logging:
```
python scripts/build_organization_hierarchy.py --validate-urls --log-level DEBUG
```

### Requirements

This script requires all dependencies of the Dataspot project. Make sure you have the environment properly set up before running it. 