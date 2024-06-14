"""TODO: Add more tests"""
import pytest

from synapseclient.services import json_schema


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


@pytest.mark.parametrize(
    "org_name",
    ["foo", "123foo"],
    ids=["name too short", "name can't start with number"],
)
def test_json_schema_organization_bad_name(org_name):
    with pytest.raises(ValueError):
        json_schema.JsonSchemaOrganization(
            name=org_name,
        )
