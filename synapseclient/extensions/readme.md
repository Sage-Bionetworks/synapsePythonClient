# Synapse Extensions

The `synapseclient.extensions` module provides purpose-built Python code and scripts that extend the core functionality of the Synapse Python Client. These extensions are designed to be easily shared and executed for specialized workflows and use cases.

## Overview

Extensions are organized into focused modules that address specific domain needs within the Synapse ecosystem. Each extension provides library functions for programmatic use.

## Available Extensions

### Curator

The curator extension provides comprehensive tools for metadata curation workflows in Synapse. It supports both file-based and record-based metadata management patterns with JSON schema validation.

**Key Features:**
- JSON schema binding to Synapse entities
- File-based metadata workflows with EntityViews
- Record-based metadata workflows with RecordSets
- CurationTask creation and management
- Schema registry integration

**Use Cases:**
- Annotating data files with structured metadata
- Creating normalized metadata records for datasets
- Validating metadata against JSON schemas
- Managing curation workflows for research projects

See the [curator README](curator/readme.md) for detailed documentation and usage examples.

## Installation

Extensions are included with the standard synapseclient installation:

```bash
pip install synapseclient
```

For development with pandas support:

```bash
pip install synapseclient[pandas]
```

## Development

When creating new extensions:

1. Create a new subdirectory under `extensions/`
2. Include an `__init__.py` file with public API exports
3. Provide library functions
4. Include comprehensive documentation and examples
5. Follow the established patterns from existing extensions

## Contributing

Extensions should be well-documented, tested, and follow the established patterns in this codebase. See the main repository's contribution guidelines for development standards and processes.
