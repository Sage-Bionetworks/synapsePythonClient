# Synapse Curator Extension - Developer Guide

This document describes the design principles and architecture of the `synapseclient.extensions.curator` module. For user-facing documentation, see `metadata_curation.md` in the docs folder.

## Design Goals

The curator extension is designed around three core principles:

1. **Flexibility through Configuration** - Provide extensive configuration options to accommodate diverse metadata workflows without requiring code changes
2. **Input Validation and Clear Error Handling** - Validate all inputs early and provide clear, actionable error messages when operations fail
3. **Composable Workflow Building** - Enable users to build complex curation workflows by combining simple, well-defined components

## Module Structure

The curator extension consists of three focused modules:

```
synapseclient/extensions/curator/
├── __init__.py                      # Clean public API surface
├── file_based_metadata_task.py      # File-annotation workflows
├── record_based_metadata_task.py    # Structured record workflows
└── schema_registry.py               # Schema discovery and validation
```

## Public API Design

The module exposes three main functions that follow consistent design patterns:

- **`create_file_based_metadata_task()`** - Configurable file-annotation curation workflows
- **`create_record_based_metadata_task()`** - Configurable structured-record curation workflows
- **`query_schema_registry()`** - Flexible schema discovery with custom filtering

## Configuration and Flexibility

### Extensive Parameter Control
Each function provides multiple configuration options to adapt to different use cases:

**Schema Integration**:
- Optional automatic schema binding with validation
- Support for custom schema registry tables and column mappings
- Flexible schema discovery with arbitrary filter parameters

**Workflow Customization**:
- Configurable entity naming and descriptions
- Optional UI enhancements (wiki pages, grids)
- Granular control over curation task properties

**Data Handling**:
- Flexible upsert key specifications for record-based workflows
- Configurable derived annotations
- Custom column type mappings

**Example - Flexible Schema Binding**:
```python
# Automatic binding (default)
create_record_based_metadata_task(..., bind_schema_to_record_set=True)

# Skip binding for manual control
create_record_based_metadata_task(..., bind_schema_to_record_set=False)

# Custom schema registry with filters
schemas = query_schema_registry(
    dcc="ad",              # Any column can be a filter
    datatype="Analysis",   # Multiple filters supported
    return_latest_only=True
)
```

### Input Validation Strategy

The module implements comprehensive input validation at multiple levels:

**Parameter Validation**:
- Required parameters are validated before any API calls
- Type checking and constraint validation on all inputs
- Clear error messages indicating exactly what needs to be corrected

**Schema Validation**:
- Schema URI existence verification before binding
- JSON schema structure validation
- Registry table structure verification

**Entity State Validation**:
- Folder and project existence checks
- Permission validation before resource creation
- Naming conflict detection and resolution

**Example - Validation-First Pattern**:
```python
# Parameters are validated before any API calls
record_set, task, grid = create_record_based_metadata_task(
    project_id="syn12345",           # Validated: non-empty
    folder_id="syn67890",             # Validated: non-empty
    record_set_name="Metadata",       # Validated: non-empty
    upsert_keys=["id"],               # Validated: non-empty list
    schema_uri="org-schema-v1.0.0",  # Validated: URI format
    # ... other parameters
)
```

## Error Handling Philosophy

### Clear and Actionable Errors
All error conditions are designed to provide developers with specific, actionable information:

- **What went wrong** - Clear description of the failure
- **Why it failed** - Context about the underlying cause
- **How to fix it** - Specific steps to resolve the issue

### Error Categories
The module handles several categories of errors with tailored messaging:

**Configuration Errors**: Invalid parameters, missing required values, type mismatches
- Example: `"project_id is required"`, `"upsert_keys is required and must be a non-empty list"`

**Permission Errors**: Insufficient access rights, authentication issues
- Clear indication of which resource requires additional permissions

**Resource Conflicts**: Naming collisions, existing entity conflicts
- Guidance on resolution strategies

**Schema Errors**: Invalid schema URIs, binding failures, validation errors
- Example: `"Invalid schema URI format: {schema_uri}. Expected format 'org-name-schema.name.schema-version'."`
- Example: `"Schema URI '{schema_uri}' not found in Synapse JSON schemas."`

**Network Errors**: API timeouts, connection issues, service unavailability
- Integration with Synapse's exception hierarchy for consistent error handling

## Workflow Composition Patterns

### Building Blocks for Complex Workflows
The module provides composable building blocks that can be combined to create sophisticated curation systems:

**Core Components**:
- **EntityView**: For file-based metadata visualization and annotation
- **RecordSet**: For structured record-based metadata storage
- **CurationTask**: For guided metadata curation workflows with validation
- **Folder**: For schema binding and organizational hierarchy
- **Grid**: For ValidationStatistics

### File-Based Metadata Workflow

**Purpose**: Organize and curate metadata at the file/entity level using EntityViews.

**Configuration Levers**:
- Schema binding behavior (automatic or manual)
- Wiki attachment preferences for ui
- Column selection and type mapping from JSON schemas

### Record-Based Metadata Workflow

**Purpose**: Manage structured metadata records using RecordSets and Grid views.

**Configuration Levers**:
- Schema binding toggle (`bind_schema_to_record_set`)
- Derived annotations enablement
- Upsert key specification for data merging
- Custom instructions and naming conventions

### Schema Discovery and Integration

**Purpose**: Flexible querying of schema registry tables with customizable filtering.

**Configuration Levers**:
- Custom registry table selection
- Column name mapping via `SchemaRegistryColumnConfig`
- Version filtering (latest-only or all versions)
- Dynamic filter construction using keyword arguments

## Development Philosophy

### Fail Fast with Clear Messages
The module is designed to fail quickly when issues are detected, providing clear diagnostic information to help developers resolve problems efficiently. Rather than allowing silent failures or partial operations, all validation happens upfront.

### Sensible Defaults with Override Options
All functions provide sensible defaults for common use cases while allowing complete customization when needed. For example:
- Schema binding is enabled by default but can be disabled
- Latest schema versions are returned by default but all versions can be requested
- Standard registry table structure is assumed but can be customized

### Dependency Injection for Testability
Functions accept optional `synapse_client` parameters to enable:
- Dependency injection for unit testing with mock clients
- Custom authentication configurations
- Support for multiple client instances in the same application

### Type Safety and Developer Experience
- Comprehensive type hints throughout for IDE support and early error detection
- Self-documenting function signatures
- Integrated logging at each workflow step for observability
- Resource IDs returned for tracking and debugging

## Integration Points

### Synapse Platform Integration
The module seamlessly integrates with core Synapse components:
- **Entity Management** - Folders, Files, EntityViews
- **Metadata Systems** - RecordSets, Grid views, annotations
- **Schema Services** - JSON schema binding and validation via `synapseclient.services.json_schema`
- **Curation Framework** - CurationTasks with `FileBasedMetadataTaskProperties` and `RecordBasedMetadataTaskProperties`

### Client Configuration
The `synapse_client` parameter provides maximum flexibility:
- Use an explicitly passed client instance
- Leverage the default cached client
- Support custom authentication configurations
- Enable testing with mock clients

## Extension Points

### Custom Schema Registries
The schema registry system is designed to work with custom tables and column structures, allowing organizations to maintain their own schema catalogs:

```python
schemas = query_schema_registry(
    custom_table_id="syn12345",
    column_config=SchemaRegistryColumnConfig(
        dcc_column="organization",
        datatype_column="type"
    )
)
```

### Workflow Type Extension
The modular architecture supports adding new workflow types while maintaining backward compatibility:
- Follow established patterns for validation and error handling
- Export new functions through `__init__.py`
- Maintain consistent parameter naming and structure
