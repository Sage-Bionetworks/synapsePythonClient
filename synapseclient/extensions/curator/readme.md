# Synapse Curator Extension - Developer Guide

This document describes the design principles and architecture of the `synapseclient.extensions.curator` module. For user-facing documentation, see `metadata_curation.md` in the docs folder.

## Design Goals

The curator extension is designed around three core principles:

1. **Flexibility through Configuration** - Provide extensive configuration options to accommodate diverse metadata workflows without requiring code changes
2. **Input Validation and Clear Error Handling** - Validate all inputs early and provide clear, actionable error messages when operations fail
3. **Composable Workflow Building** - Enable users to build complex curation workflows by combining simple, well-defined components

## Module Structure

The curator extension consists of four focused modules:

```
synapseclient/extensions/curator/
├── __init__.py                      # Clean public API surface
├── file_based_metadata_task.py      # File-annotation workflows
├── record_based_metadata_task.py    # Structured record workflows
├── schema_registry.py               # Schema discovery and validation
└── schema_generation.py             # Data model and JSON Schema generation
```

## Public API Design

The module exposes five main functions that follow consistent design patterns:

**Metadata Curation Workflows:**
- **`create_file_based_metadata_task()`** - Configurable file-annotation curation workflows
- **`create_record_based_metadata_task()`** - Configurable structured-record curation workflows
- **`query_schema_registry()`** - Flexible schema discovery with custom filtering

**Data Model and Schema Generation:**
- **`generate_jsonld()`** - Convert CSV data models to JSON-LD format with validation
- **`generate_jsonschema()`** - Generate JSON Schema validation files from data models

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

### Data Model and Schema Generation

**Purpose**: Create and validate data models, then generate JSON Schema validation files.

The schema generation workflow consists of two key functions that work together:

#### JSON-LD Data Model Generation (`generate_jsonld`)

Converts CSV-based data model specifications into standardized JSON-LD format with comprehensive validation:

**Input Requirements**:
- CSV file with attributes, validation rules, dependencies, and valid values
- Columns defining display names, descriptions, requirements, and relationships

**Validation Performed**:
- Required field presence checks
- Dependency cycle detection (ensures valid DAG structure)
- Blacklisted character detection in display names
- Reserved name conflict checking
- Graph structure validation

**Configuration Levers**:
- Label format selection (`class_label` vs `display_label`)
- Custom output path or automatic naming
- Comprehensive error and warning logging

**Output**: JSON-LD file suitable for schema generation and other data model operations

#### JSON Schema Generation (`generate_jsonschema`)

Generates JSON Schema validation files from JSON-LD data models, translating validation rules into schema constraints:

**Supported Validation Rules**:
- Type validation (string, number, integer, boolean)
- Enum constraints from valid values
- Required field enforcement (including component-specific requirements)
- Range constraints (`inRange` → min/max)
- Pattern matching (`regex` → JSON Schema patterns)
- Format validation (`date`, `url`)
- Array handling (`list` rules)
- Conditional dependencies (if/then schemas)

**Configuration Levers**:
- Component selection (specific data types or all components)
- Label format for property names
- Custom output directory structure
- Component-based rule application using `#Component` syntax

**Output**: JSON Schema files for each component, enabling validation of submitted manifests

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
