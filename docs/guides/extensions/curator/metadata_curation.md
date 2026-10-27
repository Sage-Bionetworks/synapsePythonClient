# How to Create and Manage Metadata Workflows in Curator

This guide explains how **Curator administrators** create and manage workflows for collecting metadata in Synapse.

## About Curator

Curator is a Synapse tool for collecting and managing metadata through a *spreadsheet-style interface*.

**Curator administrators** create curation tasks, define metadata requirements using a **JSON Schema**, and choose how metadata is stored: as structured records in a `RecordSet` or as annotations on individual files.

**Data contributors** complete these tasks by entering and updating metadata through the Curator web application or programmatically. If you are a contributor working on an existing task, see [How to Enter and Update Metadata for a Curation Task](metadata_contribution.md).

### In this guide, you will learn how to:

- Choose between record-based and file-based metadata workflows.
- Select a JSON Schema to define metadata requirements.
- Create a curation task for either workflow.
- Configure task parameters and understand the resulting Synapse objects.

## Types of Metadata Workflows

Curator supports **two types of metadata workflows**:

| Workflow | What each row represents | Where metadata is stored |
| --- | --- | --- |
| **Record-based metadata** (*structured records*) | One record, such as a participant or biospecimen | A `RecordSet` |
| **File-based metadata** (*file annotations*) | One Synapse file | Annotations on the individual file |

### Record-based metadata: Structured records

Use **record-based metadata** to manage information about participants, samples, or other items that do not need to correspond to individual data files.

Each row represents one record. The records are stored together in a `RecordSet`, rather than as annotations on individual files.

Records are uniquely identified by one or more **key columns**, configured using `upsert_keys`. These keys allow contributors to add new records or update matching records without creating duplicates.

### File-based metadata: File annotations

Use **file-based metadata** to describe individual files stored in Synapse. Each row represents a file, and its metadata is stored directly on that file as annotations.

!!! tip

    **You can use both workflows in the same Synapse project.** For example, store biospecimen details once in a `RecordSet`, then use a shared identifier such as `sampleId` to associate multiple files with that biospecimen.

## Prerequisites

Before creating a curation task, make sure you have the following:

- **Synapse account:** Create a Synapse account and complete the Synapse Certification Quiz.
- **Synapse Python Client:** [Install the client](https://python-docs.synapse.org/en/stable/tutorials/installation/) with the Curator extras (`pip install --upgrade "synapseclient[curator]"`) and [configure your credentials](https://python-docs.synapse.org/en/stable/tutorials/authentication/).
- **Project and folder permissions:** Have **Edit permissions or higher** for the Synapse project and folder where you will manage metadata. Note the folder's Synapse ID for use in the examples.
- **JSON Schema:** Browse [existing schemas](https://sage-bionetworks.github.io/core-models/) or create your own. You will need a **versioned schema URI**, such as `MyOrganization-MySchema-1.0.0`. For details, see the [Schema Operations Guide](https://python-docs.synapse.org/en/stable/guides/extensions/curator/schema_operations/).

> [!NOTE]
> Schema URIs and Synapse IDs in the examples below are *placeholders*. Replace them with values from your own Synapse project.

## Create a Record-Based Metadata Task

This example creates a `RecordSet` to store biospecimen metadata and a corresponding `CurationTask` for contributors.

> [!IMPORTANT]
> **Before running:** Replace `folder_id` and `schema_uri`. Make sure `upsert_keys` and `column_order` use property names defined in your registered JSON Schema.

```python
from synapseclient import Synapse
from synapseclient.extensions.curator import create_record_based_metadata_task

# Authenticate with Synapse using your configured credentials.
syn = Synapse()
syn.login()

# Create a RecordSet and a task for biospecimen metadata.
record_set, curation_task = create_record_based_metadata_task(
    # Destination folder ID. Edit permissions or higher are required.
    folder_id="syn12345678",

    # Name and description of the RecordSet that stores the metadata.
    record_set_name="Biospecimen_Metadata",
    record_set_description="Metadata describing the biospecimens in this study",

    # Name shown to contributors. An existing task with the same name
    # in the project may be updated instead of creating a new task.
    curation_task_name="Curate Biospecimen Metadata",

    # Schema property (or properties) uniquely identifying each record.
    # Existing matching records are updated; new keys create new records.
    upsert_keys=["sampleId"],

    # Instructions shown to contributors when completing the task.
    instructions=(
        "Enter one record per biospecimen. Complete all required fields and use a unique sampleId for each record."
    ),

    # URI of the registered JSON Schema. Replace this example URI.
    schema_uri="MyOrganization-BiospecimenMetadata-1.0.0",

    # True (default): Bind the schema to the RecordSet for validation.
    # False: Do not bind the schema to the RecordSet.
    bind_schema_to_record_set=True,

    # True: Enable schema-derived annotation values.
    # False (default): Disable derived annotations.
    enable_derived_annotations=False,

    # Optional Synapse user or team principal ID; None leaves it unassigned.
    assignee_principal_id=None,

    # Suggested access mode for the grid session when it is opened.
    # Options: "SESSION_OWNER", "SOURCE_BENEFACTOR", or None.
    authorization_mode="SOURCE_BENEFACTOR",

    # Optional order of columns after the pinned upsert key(s).
    # All names must match schema properties; remaining columns follow.
    column_order=["participantId", "specimenType", "tissue"],

    # Use the authenticated Synapse client created above.
    synapse_client=syn,

    # False: Return only RecordSet and CurationTask objects.
    # True: Also create and return a Grid (deprecated behavior).
    create_grid=False,
)

# Display IDs for the resources created or updated.
print(f"RecordSet ID: {record_set.id}")
print(f"CurationTask ID: {curation_task.task_id}")
```
**Example output**

```text
Welcome, <Your Name>!

Extracted schema properties and created template: ['sampleId', 'participantId', 'specimenType', 'tissue', ...]
Uploading to Synapse storage: 100%|██████████|
Created RecordSet with ID: syn12345678 in folder syn12345678
Bound schema MyOrganization-BiospecimenMetadata-1.0.0 to RecordSet ID: syn51234567
Created CurationTask (1234) with name Curate Biospecimen Metadata
RecordSet ID: syn12345678
CurationTask ID: 1234
```

> [!TIP]
> **Column placement:** The upsert key (`sampleId`) remains the leftmost column. Columns listed in `column_order` appear next, followed by any remaining schema properties.

## Create a File-Based Metadata Task

This example creates an `EntityView` of files in a folder and a corresponding `CurationTask` for annotating them.


> [!IMPORTANT]
> - **Set `return_entities=True`.** Returning objects is planned to become the default in **v5.0.0**, when `return_entities` will be removed.
> 
> - **Before running:** Replace `folder_id` and `schema_uri`. Update `column_order` to match available view columns. This example assumes your schema includes `sampleId`, `assay`, and `fileFormat`.
> 
> - **Add at least one file first:** Make sure the folder contains at least one file before you create a file-based metadata task. The `EntityView` lists the files in the folder, so contributors will have nothing to annotate if the folder is empty.

```python
from synapseclient import Synapse
from synapseclient.extensions.curator import create_file_based_metadata_task
from synapseclient.models import ViewTypeMask

# Authenticate with Synapse using your configured credentials.
syn = Synapse()
syn.login()

# Create an EntityView and a task for file annotations.
entity_view, curation_task = create_file_based_metadata_task(
    # Folder containing the files to be annotated.
    folder_id="syn87654321",

    # Task name and contributor-facing instructions.
    curation_task_name="Curate File Metadata",
    instructions=(
        "Annotate each file according to the schema requirements. Use sampleId to identify the biospecimen associated with each file."
    ),

    # True: Attach a wiki containing the EntityView to the folder.
    # False (default): Do not attach a wiki.
    attach_wiki=False,

    # Name of the EntityView created for the task.
    entity_view_name="Study File Metadata",

    # URI of the registered JSON Schema. Replace this example URI.
    schema_uri="MyOrganization-FileMetadata-1.0.0",

    # True: Enable schema-derived annotations when binding the schema.
    # False (default): Disable derived annotations.
    enable_derived_annotations=False,

    # Optional Synapse user or team principal ID; None leaves it unassigned.
    assignee_principal_id=None,

    # Include files in the EntityView (the standard file-based setting).
    view_type_mask=ViewTypeMask.FILE,

    # Suggested access mode when the grid session is opened.
    authorization_mode="SOURCE_BENEFACTOR",

    # True: Return EntityView and CurationTask objects rather than IDs.
    return_entities=True,

    # Optional order after the pinned name and id columns.
    column_order=["sampleId", "assay", "fileFormat"],

    # Use the authenticated Synapse client created above.
    synapse_client=syn,
)

# Display IDs for the resources created or updated.
print(f"EntityView ID: {entity_view.id}")
print(f"CurationTask ID: {curation_task.task_id}")
```

**Example output**

```text
Welcome, <Your Name>!

Attempting to bind schema MyOrganization-FileMetadata-1.0.0 to folder syn87654321.
Successfully bound schema MyOrganization-FileMetadata-1.0.0 to folder syn87654321.
Attempting to create entity view.
[syn23456789:Study File Metadata]: (Column Order): ['sampleId', 'assay', 'fileFormat', ..., 'name', 'id', ...]
/entity/syn23456789/table/transaction/async: 100%|██████████| 1.00/1.00 [00:01<00:00]
[syn23456789:Study File Metadata]: (Column Order): ['name', 'id', 'sampleId', 'assay', 'fileFormat', ...]
/entity/syn23456789/table/transaction/async: 100%|██████████| 1.00/1.00 [00:01<00:00]
Created entity view.
Attempting to get the attached schema.
Schema retrieval successful
Attempting to get the Synapse ID of the provided folders project.
Got the Synapse ID of the provided folders project.
Attempting to create the CurationTask.
Created the CurationTask.
EntityView ID: syn23456789
CurationTask ID: 2345
```

Your name, IDs, and column lists will differ. The last two lines come from the `print` statements in the script. The earlier lines are log messages from the client, and their wording may vary between client versions.

- **Bind schema:** The schema is bound to your folder. These lines appear when you provide `schema_uri`.
- **Create entity view:** The `EntityView` is created. The `(Column Order)` line appears twice: first with the initial columns, then with `name` and `id` pinned first, followed by your `column_order` columns.
- **Get the attached schema:** The client reads back the schema bound to the folder.
- **Create the CurationTask:** The task contributors will see is created.

## Parameter and Property Reference

Use the tables below to configure a workflow or look up a parameter used in either example. **Required** parameters must be supplied; optional parameters show their documented defaults.

### Parameters Shared by Both Functions

These parameters are accepted by both `create_record_based_metadata_task()` and `create_file_based_metadata_task()`.

| Parameter | Type / default | Description and options |
| --- | --- | --- |
| `folder_id` | `str` — **required** | Synapse folder ID. For record-based tasks, the destination for the `RecordSet`; for file-based tasks, the folder containing files to curate. |
| `curation_task_name` | `str` — **required** | Name of the task. Use a unique name within the project to create a new task. |
| `instructions` | `str` — **required** | Contributor-facing instructions describing each row and the information to enter. |
| `schema_uri` | `str` — **required** for record-based; `str` or `None` (default: `None`) for file-based | URI of a registered JSON Schema. For file-based tasks, providing a URI binds the schema to the folder before creating the view. |
| `enable_derived_annotations` | `bool` — default: `False` | Set `True` to enable schema-derived annotations during schema binding. See [Derived annotations](#derived-annotations). |
| `assignee_principal_id` | `str`, `int`, or `None` — default: `None` | Synapse principal ID of the user or team assigned to the task. `None` leaves it unassigned. |
| `authorization_mode` | `AuthorizationMode`, `str`, or `None` — default: `None` | Suggested grid access mode: `"SESSION_OWNER"`, `"SOURCE_BENEFACTOR"`, or `None`. See [Grid access options](#grid-access-options). |
| `column_order` | `list[str]` or `None` — default: `None` | Desired left-to-right column order after pinned columns. Unlisted columns remain visible. See [Column ordering](#column-ordering). |
| `synapse_client` | `Synapse` or `None` — default: `None` | Client used for the operation. Passing an authenticated client makes the connection explicit. |

> [!NOTE]
> **Additional behavior:** Reusing an existing task name may update the matching task. If `synapse_client` is omitted, the last-created client is used when client caching is enabled. For file-based tasks, omit `schema_uri` only when the folder already has an applicable schema.

### Record-Based Parameters

The following parameters apply only to `create_record_based_metadata_task()`.

| Parameter | Type / default | Description and options |
| --- | --- | --- |
| `record_set_name` | `str` — **required** | Name of the `RecordSet` used to store structured metadata. |
| `record_set_description` | `str` — **required** | Description of the contents of the `RecordSet`. |
| `upsert_keys` | `list[str]` — **required** | One or more schema columns that uniquely identify each record. Multiple columns form a *composite key*. |
| `bind_schema_to_record_set` | `bool` — default: `True` | `True` binds the schema to the `RecordSet` for validation; `False` skips the binding. |
| `project_id` | `str` or `None` — default: `None` | **Deprecated.** The project is inferred from `folder_id`; omit this parameter. |
| `create_grid` | `bool` — default: `True` | **Deprecated.** `True` creates a `Grid` and returns it as a third object; `False` returns only the `RecordSet` and `CurationTask`. |

> [!IMPORTANT]
> **Upsert keys:** `upsert_keys` must be nonempty and refer to properties in the schema. Even if `bind_schema_to_record_set=False`, a `schema_uri` is still required to build the metadata template.

**How keys affect grid CSV imports:** When contributors use `Grid.import_csv()`, a new key adds a record and an existing key updates the matching record; records absent from the import remain unchanged. When the key uses multiple columns, contributors must complete *all* of them. Uploading a CSV directly by setting `RecordSet.path` and calling `RecordSet.store()` replaces the RecordSet's data file instead of upserting its rows.

> [!NOTE]
> The supplied API reference identifies `project_id` and `create_grid` as deprecated and scheduled for removal in **Synapse Python Client v5.0.0**.

### File-Based Parameters

The following parameters apply only to `create_file_based_metadata_task()`.

| Parameter | Type / default | Description and options |
| --- | --- | --- |
| `attach_wiki` | `bool` — default: `False` | `True` attaches a wiki containing the `EntityView` to the folder; `False` does not. |
| `entity_view_name` | `str` — default: `"JSON Schema view"` | Name of the `EntityView` created for the task. |
| `view_type_mask` | `ViewTypeMask` or `int` — default: `ViewTypeMask.FILE` | Controls which object types are included in the view. Use `ViewTypeMask.FILE` for a standard file-annotation task. |
| `return_entities` | `bool` — default: `False` | `False` returns view and task IDs; `True` returns `EntityView` and `CurationTask` objects. |

> [!NOTE]
> The supplied API reference indicates that returning objects is planned to become the default in **v5.0.0**, when `return_entities` is also scheduled for removal.

### Grid Access Options

`authorization_mode` stores a **suggested access mode** on the task. The selected mode is applied when a client creates the grid session; it does *not* immediately create a session.

| Value | Meaning |
| --- | --- |
| `"SESSION_OWNER"` or `AuthorizationMode.SESSION_OWNER` | Restricts access to the user or team that owns the session. Team members can join a team-owned session. |
| `"SOURCE_BENEFACTOR"` or `AuthorizationMode.SOURCE_BENEFACTOR` | Allows users with **Edit** permissions on the source data to collaborate in the session. |
| `None` | Does not store an explicit recommendation; clients typically fall back to their usual private-session behavior for the current user. |

> [!WARNING]
> **Changing access mode:** Changing the authorization mode resets the active session link. A new grid session must be opened before curation continues.

### Column Ordering

Use `column_order` to control column placement without listing every column. Do not assume properties will appear in the same order as they are written in the JSON Schema.

| Setting or rule | Behavior |
| --- | --- |
| `None` | No custom ordering is requested. |
| List of column names | Listed columns appear in that order, immediately after pinned columns. |
| Unlisted columns | Remain visible and follow in their existing relative order. |
| **Record-based pinned columns** | Columns in `upsert_keys` remain first. |
| **File-based pinned columns** | `name` and `id` remain first. |
| Pinned column included in the list | Stays in its pinned position without duplication. |
| Unknown column name | Raises a `ValueError`. |
| File-based system columns | Available columns such as `createdBy` and `modifiedOn` can be placed after pinned columns. |

### Derived Annotations

**Derived annotations** are metadata values calculated from rules in a bound JSON Schema, such as `default` values, `const` values, or conditional rules.

- **`enable_derived_annotations=True`:** Enables derived annotations during schema binding.
- **`enable_derived_annotations=False`:** Disables this behavior (*default*).

Derived annotations are generated only when annotation data is valid against the bound schema.

!!! tip

    For a first curation task, use `False` unless the schema is designed to generate derived annotation values.

### View Type Options

`ViewTypeMask` identifies the types of Synapse objects an `EntityView` can include. The actual objects returned also depend on the view's scope. For a standard file-based workflow, use **`ViewTypeMask.FILE`**.

| Enum member | Integer value | Object type |
| --- | ---: | --- |
| `ViewTypeMask.FILE` | 1 | Files |
| `ViewTypeMask.PROJECT` | 2 | Projects |
| `ViewTypeMask.TABLE` | 4 | Tables |
| `ViewTypeMask.FOLDER` | 8 | Folders |
| `ViewTypeMask.VIEW` | 16 | EntityViews |
| `ViewTypeMask.DOCKER` | 32 | Docker repositories |
| `ViewTypeMask.SUBMISSION_VIEW` | 64 | SubmissionViews |
| `ViewTypeMask.DATASET` | 128 | Datasets |
| `ViewTypeMask.DATASET_COLLECTION` | 256 | DatasetCollections |
| `ViewTypeMask.MATERIALIZED_VIEW` | 512 | MaterializedViews |

!!! note

    Multiple view types can be combined using the bitwise OR (`|`) operator. The examples in this guide use only `ViewTypeMask.FILE`.

### Return Values

The number and type of values returned by each helper depend on its compatibility settings.

| Function and setting | Returned values, in order |
| --- | --- |
| Record-based with `create_grid=False` (*recommended*) | `RecordSet`, `CurationTask` |
| Record-based with `create_grid=True` (*current default in supplied signature; deprecated*) | `RecordSet`, `CurationTask`, `Grid` |
| File-based with `return_entities=True` (*recommended*) | `EntityView`, `CurationTask` |
| File-based with `return_entities=False` (*current default in supplied signature; deprecated*) | `EntityView` ID, task ID |

!!! note

    **Keep `return_entities=True` and `create_grid=False`.** The current defaults (`return_entities=False` and `create_grid=True`) are deprecated and scheduled for removal in **Synapse Python Client v5.0.0**. Setting these values explicitly keeps your scripts working the same way after that release.

### Created Objects and Workflow Properties

These properties describe the objects returned by the helper functions; they are *not* additional function parameters.

#### Identifiers

| Property | Description |
| --- | --- |
| `record_set.id` | Synapse ID of the created `RecordSet`. |
| `entity_view.id` | Synapse ID of the created `EntityView`. |
| `curation_task.task_id` | Numeric ID of the curation task. |

#### Curation task properties

| Property | Description |
| --- | --- |
| `curation_task.data_type` | Task's data-type/name field, populated from `curation_task_name`. |
| `curation_task.project_id` | Synapse ID of the containing project. *Distinct from the deprecated helper parameter.* |
| `curation_task.instructions` | Instructions shown to contributors. |
| `curation_task.task_properties` | Configuration connecting the task to its record-based or file-based source. |
| `curation_task.task_properties.record_set_id` | Record-based: Synapse ID of the `RecordSet`. |
| `curation_task.task_properties.upload_folder_id` | File-based: folder associated with the files. |
| `curation_task.task_properties.file_view_id` | File-based: Synapse ID of the `EntityView`. |
| `curation_task.task_properties.suggested_authorization_mode` | Access-mode recommendation set by `authorization_mode`. |

### Record-Based Validation Properties

After metadata is exported from the grid back to a schema-bound `RecordSet`, Synapse generates a saved **row-level validation report**. Administrators can use the following fields to review the results.

| Property or report column | Description |
| --- | --- |
| `record_set.validation_file_handle_id` | Identifies the saved validation report after an export generates validation results. |
| `row_index` | Identifies the row associated with a validation result. |
| `is_valid` | Whether the row passed validation: `True` or `False`. |
| `validation_error_message` | Summary of validation errors for the row. |
| `all_validation_messages` | All schema-validation messages for the row. |

After contributors synchronize (push) the grid back to the `RecordSet`, fetch the latest version before retrieving the detailed report. Import `RecordSet` with `from synapseclient.models import RecordSet`, then use `record_set = RecordSet(id=record_set.id).get(synapse_client=syn)` to load the latest `validation_file_handle_id`. Retrieve the report with `record_set.get_detailed_validation_results(synapse_client=syn)`. If the method returns `None`, confirm that the schema is bound and the grid changes have been pushed to the `RecordSet`.
