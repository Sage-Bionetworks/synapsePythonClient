"""TODO: Add more tests"""
from synapseclient.services import json_schema


def test_json_schema_organization():
    org = json_schema.JsonSchemaOrganization(
        name="temp",
    )
    assert org.name == "temp"
    assert org.id is None
    assert org.created_on is None
    assert org.created_by is None
    assert org._json_schemas == dict()
    assert org._raw_json_schemas == dict()
