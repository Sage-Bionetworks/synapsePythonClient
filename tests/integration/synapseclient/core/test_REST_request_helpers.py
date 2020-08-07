"""
This file contains integration tests for methods in the :py:class:`Synapse` class that act as helpers for making REST
requests to the Synapse backend
"""

from synapseclient import Column


def test_createColumns(syn):
    columns_to_create = [Column(name="FirstTestColumn", columnType="INTEGER"), Column(name="SecondTestColumn",
                                                                                      columnType="DOUBLE")]
    created_columns = syn.createColumns(columns_to_create)
    assert len(columns_to_create) == len(created_columns)
    for col_to_create, created_col in zip(columns_to_create, created_columns):
        assert 'id' in created_col
        assert set(col_to_create.items()).issubset(set(created_col.items()))
