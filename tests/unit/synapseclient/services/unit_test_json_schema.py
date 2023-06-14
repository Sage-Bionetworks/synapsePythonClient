"""TODO: Add more tests"""
from synapseclient.services import json_schema
import pytest


def test_json_schema_organization():
    org = json_schema.JsonSchemaOrganization(
        name="foo123",
    )
    assert org.name == "foo123"
    assert org.id is None
    assert org.created_on is None
    assert org.created_by is None
    assert org._json_schemas == dict()
    assert org._raw_json_schemas == dict()


def test_json_schema_organization_bad_name():
    with pytest.raises(ValueError):

        # Name is too short
        json_schema.JsonSchemaOrganization(
            name="foo",
        )

        # Name cannot start with a number
        json_schema.JsonSchemaOrganization(
            name="123foo",
        )
