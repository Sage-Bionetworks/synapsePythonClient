# CSV data model description

The Curator-Extension (formerly Schematic) data model is used to create JSON Schemas for Curator. See [JSON Schema documentation](https://json-schema.org/). This is used for the DCCs that prefer working in a tabular format (CSV) over JSON. Data mangers will create their data model(s) in the CSV format specified below, and then use the Curator-Extension in the Synapse Python Client to convert to JSON Schema.A JSON Schema is made up of one data type(for example a person) and the attributes that describe the data type (for example age and gender).

## Data model columns

The CSV data model will describe one or more data types. Each row describes either a data type, or an attribute.

Data types:

- must have a unique name in the Attribute column
- must have at least one attribute in the DependsOn column
- may have a description

Attributes:

- must have a unique name in the Attribute column
- may have all other columns besides DependsOn

The following data model has one data type, Person, and that data type has one attribute, Gender.

| Attribute | DependsOn |
|---|---|
| Person    | "Gender"  |
| Gender    |           |

Converting the above data model to JSON Schema results in:

```json
{
  "description": "TBD",
  "properties": {
    "Gender": {
      "description": "TBD",
      "title": "Gender"
    }
  }
}
```

### Attribute

This is used to name the data type/attribute being described on this line. This should be a unique identifier in the file. For attributes this will be translated as the title in the JSON Schema.

### DependsOn

The set of of attributes this data type has. These must be attributes elsewhere in the file. Example: "Patient ID, Sex, Year of Birth, Diagnosis" Each attribute will appear in the properties of the JSON Schema.

### Description

Optional Description of the datatype/attribute. This will be translated as description in the JSON Schema. If left blank, this will be filled with ‘TBD’.

### Valid Values

Set of possible values for the current attribute. This attribute be an enum in the JSON Schema, with the values here as the enum values. See [enum](https://json-schema.org/understanding-json-schema/reference/enum#enumerated-values).

Data Model:

| Attribute | DependsOn | Valid Values          |
|---|---|---|
| Person    | "Gender"  |                       |
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

Whether a value must be set for the current attribute. This field is boolean, i.e. valid values are ‘TRUE’ and ‘FALSE’. If set to ‘TRUE’, a value must be provided for this attribute. If set to ‘FALSE’, providing a value is considered optional. All attributes that are required will appear in the required list in the JSON Schema. See [required](https://json-schema.org/understanding-json-schema/reference/object#required).

Data Model:

| Attribute | DependsOn      | Required |
|---|---|---|
| Person    | "Gender, Age"  |          |
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
    "Gender": {
      "description": "TBD",
      "title": "Age"
    }
  },
  "required": ["Gender"]
}
```


### columnType

The data type this of this attribute. See [type](https://json-schema.org/understanding-json-schema/reference/type). Must be one of:

- "string"
- "number"
- "integer"
- "boolean"
- "string_list"
- "integer_list"
- "boolean_list"

Data Model:

| Attribute | DependsOn         | columnType  |
|---|---|---|
| Person    | "Gender, Assays"  |             |
| Gender    |                   | string      |
| Assays    |                   | string_list |

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
    "Assays": {
      "description": "TBD",
      "title": "Assays",
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  }
}
```

### Format

The format of this attribute. The type of this attribute must be "string" or "string_list". The value of this column will be the [format](https://json-schema.org/understanding-json-schema/reference/type#format) of this attribute in the JSON Schema. Must be one of:

- "date-time"
- "email"
- "hostname"
- "ipv4"
- "ipv6"
- "uri"
- "uri-reference"
- "uri-template"
- "json-pointer"
- "date"
- "time"
- "regex"
- "relative-json-pointer"

Data Model:

| Attribute | DependsOn       | columnType  | Format |
|---|---|---|---|
| Person    | "Gender, Date"  |             |        |
| Gender    |                 | string      |        |
| Date      |                 | string      | date   |

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
    "Date": {
      "description": "TBD",
      "title": "Date",
      "type": "string",
      "format": "date"
    }
  }
}
```

### Pattern

The regex pattern this attribute match. The type of this attribute must be "string" or "string_list". The value of this column will be the [pattern](https://json-schema.org/understanding-json-schema/reference/https://json-schema.org/understanding-json-schema/reference/regular_expressions#regular-expressions) of this attribute in the JSON Schema. Must be one a legal regex as determined by the python re library.

Data Model:

| Attribute | DependsOn     | columnType  | Pattern |
|---|---|---|---|
| Person    | "Gender, ID"  |             |         |
| Gender    |               | string      |         |
| ID        |               | string      | [a-f]   |

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

The range of numeric values this attribute must be in. The type of this attribute must be "integer", "number", or "integer_list". The values in these columns will be the [range]The type of this attribute must be "string" or "string_list" in the JSON Schema. Both must be numeric values.

Data Model:

| Attribute  | DependsOn                  | columnType  | Minimum | Maximum |
|---|---|---|---|---|
| Person     | "Age, Weight, Expression"  |             |         |         |
| Age        |                            | integer     | 0       | 120     |
| Weight     |                            | number      | 0.0     |         |
| Expression |                            | number      | 0.0     | 1.0     |

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
    "Expression": {
      "description": "TBD",
      "title": "Expression",
      "type": "number",
      "minimum": 0.0,
      "maximum": 1.0
    },
  }
}
```

### Validation Rules (deprecated)

This a remnant from Schematic. t is still used(for now) to translate certain validation rules to other JSONSchema key words. If you are starting a new data model do not use it. If you have an existing data model using any of the following validation rules, follow these instructions to update it:

- list: Make sure you are using one of the list-types in the columnType column.
- regex: "regex module pattern", move the pattern part to the Pattern column.
- inRange: "inRange minimum maximum", move the minimum and the maximum to the Minimum and Maximum columns respectively.
- date: Use the Format column with value 'date'
- url: Use the Format column with value 'uri'
