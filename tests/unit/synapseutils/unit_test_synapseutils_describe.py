import sys
import tempfile
from unittest.mock import Mock, patch

from numpy import array_equal
import pandas as pd
import pytest
import synapseclient
from synapseutils import describe_functions
from synapseutils.describe_functions import _open_entity_as_df, _describe_wrapper


class TestOpenEntityAsDf:

    id = 'syn123456'
    df = pd.DataFrame(
            {'gene': ['MSN', 'CD44', 'MSN', 'CD44', 'MSN', 'CD44', 'MSN', 'CD44', 'CD44'],
             "score": [1, 2, 1, 2, 1, 2, 1, 2, 1]
             })

    csv_path = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    tsv_path = tempfile.NamedTemporaryFile(delete=False, suffix='.tsv')

    @pytest.fixture(autouse=True, scope='function')
    def init_syn(self, syn):
        self.syn = syn

    @pytest.fixture
    def setup_csv(self):
        self.df.to_csv(self.csv_path.name)
        self.csv_ent = synapseclient.File(name='file.csv',
                                          id=self.id,
                                          path=self.csv_path.name,
                                          parentId='8765')

    @pytest.fixture
    def setup_tsv(self):
        self.df.to_csv(self.tsv_path.name, sep='\t')
        self.tsv_ent = synapseclient.File(name='file.tsv',
                                          id=self.id,
                                          path=self.tsv_path.name,
                                          parentId='8765')

    def test_open_entity_as_df_csv(self, setup_csv):
        with patch.object(self.syn, "get", return_value=self.csv_ent) as mocked_get_entity:

            result_csv = _open_entity_as_df(self.syn, self.id)
            assert type(result_csv) == pd.DataFrame

            result_csv = pd.DataFrame(result_csv)
            assert array_equal(result_csv[['gene']], self.df[['gene']]) is True
            assert array_equal(result_csv[['score']], self.df[['score']]) is True

            mocked_get_entity.assert_called_once()

    def test_open_entity_as_df_tsv(self, setup_tsv):
        with patch.object(self.syn, "get", return_value=self.tsv_ent) as mocked_get_entity:

            result_tsv = _open_entity_as_df(self.syn, self.id)
            assert type(result_tsv) == pd.DataFrame

            result_tsv = pd.DataFrame(result_tsv)
            assert array_equal(result_tsv[['gene']], self.df[['gene']]) is True
            assert array_equal(result_tsv[['score']], self.df[['score']]) is True

            mocked_get_entity.assert_called_once()


class TestDescribe:
    id = 'syn123456'
    df_mixed = pd.DataFrame(
        {
            'gene': ['MSN', 'CD44', 'MSN', 'CD44', 'MSN', 'CD44', 'MSN', 'CD44', 'CD44', 'CD44'],
            "score": [1, 2, 1, 2, 1, 2, 1, 2, 1, 1],
            "related": [['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44'],
                        ['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44']],
            "presence_in_ad_brain": [True, False, True, False, True, False, True, False, False, True]
        }
    )
    if sys.version_info < (3, 7, 0):
        expected_results = {
            'gene': {
                'dtype': 'object',
                'mode': "CD44"
            },
            "score": {
                'dtype': 'int64',
                "mode": 1,
                "min": 1,
                "max": 2,
                'mean': 1.4
            },
            'related': {},
            "presence_in_ad_brain": {
                'dtype': 'bool',
                "mode": False,
                "min": False,
                "max": True,
                'mean': 0.5
            }
        }
    else:
        expected_results = {
            'gene': {
                'dtype': 'object',
                'mode': "CD44"
            },
            "score": {
                'dtype': 'int64',
                "mode": 1,
                "min": 1,
                "max": 2,
                'mean': 1.4
            },
            'related': {
                'dtype': 'object',
                'mode': ['CD44']
            },
            "presence_in_ad_brain": {
                'dtype': 'bool',
                "mode": False,
                "min": False,
                "max": True,
                'mean': 0.5
            }
        }

    def test_describe_with_mixed_series(self):

        result = _describe_wrapper(df=self.df_mixed)
        assert result == self.expected_results

    def test_describe(self):
        syn = Mock()
        with patch.object(describe_functions, "_open_entity_as_df",
                          return_value=self.df_mixed) as mock_open_entity,\
             patch.object(describe_functions, "_describe_wrapper",
                          return_value=self.expected_results) as mock_describe:
            result = describe_functions.describe(syn=syn, entity="syn1234")
            mock_open_entity.assert_called_once_with(syn=syn,
                                                     entity="syn1234")
            mock_describe.assert_called_once_with(self.df_mixed)
            assert result == self.expected_results

    def test_describe_none(self):
        """Test if data type is not supported"""
        syn = Mock()
        with patch.object(describe_functions, "_open_entity_as_df",
                          return_value=None),\
             patch.object(describe_functions,
                          "_describe_wrapper") as mock_describe:
            result = describe_functions.describe(syn=syn, entity="syn1234")
            mock_describe.assert_not_called()
            assert result is None
