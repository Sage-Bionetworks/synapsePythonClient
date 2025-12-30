# CSV data model description

The Curator-Extension (formerly Schematic) data model is used to create JSON Schemas for Curator. See [JSON Schema documentation](https://json-schema.org/). This is used for the DCCs that prefer working in a tabular format (CSV) over JSON or LinkML. A data model is created in the format specified below. Then the Curator-Extension in the Synapse Python Client can be used to convert to JSON Schema.

A link will be provided here to documentation for converting CSV data models to JSON Schema in the near future.

## Understanding the Structure

A data model describes real world entities(data types) and attributes that you want to collect data for. For example you might want to describe a Patient, and you want to collect their age, gender and name.

The CSV data model described in this tutorial formalizes this structure:

- The CSV data model describes one or more data types.
- Each row describes either a data type, or an attribute.

Here is the Patient described above represented as a CSV data model:

| Attribute | DependsOn           |
|-----------|---------------------|
| Patient   | "Age, Gender, Name" |
| Age       |                     |
| Gender    |                     |
| Name      |                     |

The end goal is to create a JSON Schema that can be used in Curator. A JSON Schema consists of only one data type and its attributes. Converting the above data model to JSON Schema results in:

```json
{
  "description": "TBD",
  "properties": {
    "Age": {
      "description": "TBD",
      "title": "Age"
    },
    "Gender": {
      "description": "TBD",
      "title": "Gender"
    },
    "Name": {
      "description": "TBD",
      "title": "Name"
    }
  }
}
```

## CSV Data model columns

Note: Individual columns are covered later on this page.

These columns must be present in your CSV data model:

- `Attribute`
- `DependsOn`
- `Description`
- `Valid Values`
- `Required`
- `Parent`
- `Validation Rules`

Defining data types:

- Put a unique data type name in the `Attribute` column.
- Put the value `DataType` in the `Parent` column.
- List at least one attribute in the `DependsOn` column (comma-separated).
- Optionally add a description to the `Description` column.

Defining attributes:

- Put a unique attribute name in the `Attribute` column.
- Leave the `DependsOn` column empty.
- All other columns are optional.

### Attribute

The name of the data type or attribute being described on this line. This should be a unique identifier in the file. For attributes this will be translated as the title in the JSON Schema.

### DependsOn

The set of of attributes this data type has. These must be attributes that exists in this data model. Each attribute will appear in the properties of the JSON Schema. This should be a comma-separated list in quotes. Example: "Patient ID, Sex, Year of Birth, Diagnosis"

### Description

A description of the datatype or attribute. This will be appear as a description in the JSON Schema. If left blank, this will be filled with ‘TBD’.

### Valid Values

Set of possible values for the current attribute. This attribute will be an enum in the JSON Schema, with the values here as the enum values. See [enum](https://json-schema.org/understanding-json-schema/reference/enum#enumerated-values). This should be a comma-separated list in quotes. Example: "Female, Male, Other"

Data Model:

| Attribute | DependsOn | Valid Values          |
|-----------|-----------|-----------------------|
| Patient   | "Gender"  |                       |
| Gender    |           | "Female, Male, Other" |

JSON Schema output:

```json
{
  "description": "TBD",
  "properties": {
    "Gender": {
      "description": "TBD",
      "title": "Gender",
      "enum": ["Female", "Male", "Other"]
    }
  }
}
```

### Required

Whether a value must be set for this attribute. This field is boolean, i.e. valid values are `TRUE` and `FALSE`. All attributes that are required will appear in the required list in the JSON Schema. See [required](https://json-schema.org/understanding-json-schema/reference/object#required).

Note: Leaving this empty is the equivalent of `False`.

Data Model:

| Attribute | DependsOn      | Required |
|-----------|----------------|----------|
| Patient   | "Gender, Age"  |          |
| Gender    |                | True     |
| Age       |                | False    |

JSON Schema output:

```json
{
  "description": "TBD",
  "properties": {
    "Gender": {
      "description": "TBD",
      "title": "Gender",
    },
    "Age": {
      "description": "TBD",
      "title": "Age"
    }
  },
  "required": ["Gender"]
}
```

### Parent

Put the value `DataType` in this column if this row is a data type. Other values are currently ignored. It is currently used to find all the data types in the data model.

### columnType

The data type of this attribute. See [type](https://json-schema.org/understanding-json-schema/reference/type).

Must be one of:

- `string`
- `number`
- `integer`
- `boolean`
- `string_list`
- `integer_list`
- `boolean_list`

Data Model:

| Attribute | DependsOn         | columnType  | Parent   |
|-----------|-------------------|-------------|----------|
| Patient   | "Gender, Hobbies" |             | DataType |
| Gender    |                   | string      |          |
| Hobbies   |                   | string_list |          |

JSON Schema output:

```json
{
  "description": "TBD",
  "properties": {
    "Gender": {
      "description": "TBD",
      "title": "Gender",
      "type": "string"
    },
    "Hobbies": {
      "description": "TBD",
      "title": "Hobbies",
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  }
}
```

### Format

The format of this attribute. See [format](https://json-schema.org/understanding-json-schema/reference/type#format) The type of this attribute must be "string" or "string_list". The value of this column will appear as the `format` of this attribute in the JSON Schema. Must be one of:

- `date-time`
- `email`
- `hostname`
- `ipv4`
- `ipv6`
- `uri`
- `uri-reference`
- `uri-template`
- `json-pointer`
- `date`
- `time`
- `regex`
- `relative-json-pointer`

Data Model:

| Attribute       | DependsOn            | columnType  | Format | Parent   |
|-----------------|----------------------|-------------|--------|----------|
| Patient         | "Gender, Birth Date" |             |        | DataType |
| Gender          |                      | string      |        |          |
| Birth Date      |                      | string      | date   |          |

JSON Schema output:

```json
{
  "description": "TBD",
  "properties": {
    "Gender": {
      "description": "TBD",
      "title": "Gender",
      "type": "string"
    },
    "Birth Date": {
      "description": "TBD",
      "title": "Birth Date",
      "type": "string",
      "format": "date"
    }
  }
}
```

### Pattern

The regex pattern this attribute must match. The type of this attribute must be `string` or `string_list`. See [pattern](https://json-schema.org/understanding-json-schema/reference/regular_expressions#regular-expressions) The value of this column will appear as the `pattern` of this attribute in the JSON Schema. Must be a legal regex pattern as determined by the python `re` library.

Data Model:

| Attribute | DependsOn     | columnType  | Pattern | Parent   |
|-----------|---------------|-------------|---------|----------|
| Patient   | "Gender, ID"  |             |         | DataType |
| Gender    |               | string      |         |          |
| ID        |               | string      | [a-f]   |          |

JSON Schema output:

```json
{
  "description": "TBD",
  "properties": {
    "Gender": {
      "description": "TBD",
      "title": "Gender",
      "type": "string"
    },
    "ID": {
      "description": "TBD",
      "title": "ID",
      "type": "string",
      "pattern": "[a-f]"
    }
  }
}
```

### Minimum/Maximum

The range that this attribute's numeric values must fall within. The type of this attribute must be "integer", "number", or "integer_list". See [range](https://json-schema.org/understanding-json-schema/reference/numeric#range)  The value of these columns will appear as the `minimum` and `maximum` of this attribute in the JSON Schema. Both must be numeric values.

Data Model:

| Attribute    | DependsOn                   | columnType  | Minimum | Maximum | Parent   |
|--------------|-----------------------------|-------------|---------|---------|----------|
| Patient      | "Age, Weight, Health Score" |             |         |         | DataType |
| Age          |                             | integer     | 0       | 120     |          |
| Weight       |                             | number      | 0.0     |         |          |
| Health Score |                             | number      | 0.0     | 1.0     |          |

JSON Schema output:

```json
{
  "description": "TBD",
  "properties": {
    "Age": {
      "description": "TBD",
      "title": "Age",
      "type": "integer",
      "minimum": 0,
      "maximum": 120
    },
    "Weight": {
      "description": "TBD",
      "title": "Weight",
      "type": "number",
      "minimum": 0.0
    },
    "Health Score": {
      "description": "TBD",
      "title": "Health Score",
      "type": "number",
      "minimum": 0.0,
      "maximum": 1.0
    }
  }
}
```

### Validation Rules (deprecated)

This is a remnant from Schematic. It is still required and in use (for now) to translate certain validation rules to other JSON Schema keywords.

If you are starting a new data model, DO NOT fill out this column, just leave it blank.

If you have an existing data model using any of the following validation rules, follow these instructions to update it:

- `list`: Make sure you are using one of the list-types in the `columnType` column.
- `regex`: `regex <module> <pattern>`, move the `<pattern>` to the `Pattern` column.
- `inRange`: `inRange <minimum> <maximum>`, move the `<minimum>` and/or the `<maximum>` to the `Minimum` and `Maximum` columns respectively.
- `date`: Use the `Format` column with value `date`
- `url`: Use the `Format` column with value `uri`

## Conditional dependencies

The `DependsOn`, `Valid Values` and `Parent` columns can be used together to flexibly define conditional logic for determining the relevant attributes for a data type.

In this example we have the `Patient` data type. The `Patient` can be diagnosed as healthy or with cancer. For Patients with cancer we also want to collect info about their cancer type, and any cancers in their family history.

Data Model:

| Attribute      | DependsOn                     | Valid Values        | Required | columnType  | Parent   |
|----------------|-------------------------------|---------------------|----------|-------------|----------|
| Patient        | "Diagnosis"                   |                     |          |             | DataType |
| Diagnosis      |                               | "Healthy, Cancer"   | True     | string      |          |
| Cancer         | "Cancer Type, Family History" |                     |          |             |          |
| Cancer Type    |                               | "Brain, Lung, Skin" | True     | string      |          |
| Family History |                               | "Brain, Lung, Skin" | True     | string_list |          |

To demonstrate this, see the above example with the `Patient` and `Cancer` data types:

- `Diagnosis` is an attribute of `Patient`.
- `Diagnosis` has `Valid Values` of `Healthy` and `Cancer`.
- `Cancer` is also a data type.
- `Cancer Type` and `Family History` are attributes of `Cancer` and are both required.
- `Patient` is a data type, but `Cancer` is not, as defined by the `Parent` column.

As a result of the above data model, in the JSON Schema:

- `Cancer Type` and `Family History` become properties of `Patient`.
- For a given `Patient`, if `Diagnosis` == `Cancer` then `Cancer Type` and `Family History` become required for that `Patient`.
- The conditional logic is contained in the `allOf` array.

```JSON
{
  "description": "TBD",
  "properties": {
    "Diagnosis": {
      "description": "TBD",
      "enum": ["Cancer", "Healthy"],
      "title": "Diagnosis",
      "type": "string"
    },
    "Cancer Type": {
      "description": "TBD",
      "enum": ["Brain","Lung","Skin"],
      "title": "Cancer Type",
      "type": "string"
    },
    "Family History": {
      "description": "TBD",
      "title": "Family History",
      "type": "array",
      "items": {
        "type": "string",
        "enum": ["Brain","Lung","Skin"],
      }
    }
  },
  "required": ["Diagnosis"],
  "allOf": [
    {
      "if": {
        "properties": {
          "Diagnosis": {
            "enum": [
              "Cancer"
            ]
          }
        }
      },
      "then": {
        "required": ["Cancer Type", "Family History"]
      }
    }
  ]
}
```
