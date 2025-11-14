"""Unit tests for DataModelGraphExplorer"""

import pytest

from synapseclient.extensions.curator.schema_generation import (
    AtomicColumnType,
    ColumnType,
    DataModelGraphExplorer,
    ListColumnType,
)


@pytest.mark.parametrize(
    "node_label, relationship, expected_nodes",
    [
        # rangeValue will get an attributes valid values
        (
            "CancerType",
            "rangeValue",
            ["Breast", "Colorectal", "Lung", "Prostate", "Skin"],
        ),
        (
            "FamilyHistory",
            "rangeValue",
            ["Breast", "Colorectal", "Lung", "Prostate", "Skin"],
        ),
        ("FileFormat", "rangeValue", ["BAM", "CRAM", "CSV/TSV", "FASTQ"]),
        # requiresDependency will get an components attributes
        (
            "Patient",
            "requiresDependency",
            ["Component", "Diagnosis", "PatientID", "Sex", "YearofBirth"],
        ),
        (
            "Biospecimen",
            "requiresDependency",
            ["Component", "PatientID", "SampleID", "TissueStatus"],
        ),
        # requiresDependency will get an attributes dependencies
        ("Cancer", "requiresDependency", ["CancerType", "FamilyHistory"]),
    ],
)
def test_get_adjacent_nodes_by_relationship(
    dmge: DataModelGraphExplorer,
    node_label: str,
    relationship: str,
    expected_nodes: list[str],
) -> None:
    assert (
        sorted(dmge.get_adjacent_nodes_by_relationship(node_label, relationship))
        == expected_nodes
    )


@pytest.mark.parametrize(
    "node_label, column_type",
    [
        ("Stringtype", AtomicColumnType.STRING),
        ("Stringtypecaps", AtomicColumnType.STRING),
        ("List", ListColumnType.STRING_LIST),
        ("ListBoolean", ListColumnType.BOOLEAN_LIST),
        ("ListInteger", ListColumnType.INTEGER_LIST),
    ],
)
def test_get_node_column_type(
    dmge: DataModelGraphExplorer, node_label: str, column_type: ColumnType
) -> None:
    assert dmge.get_node_column_type(node_label) == column_type
