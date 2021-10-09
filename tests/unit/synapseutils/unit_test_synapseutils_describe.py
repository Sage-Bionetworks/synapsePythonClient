import tempfile
from unittest.mock import patch

from numpy import array_equal
import pandas as pd
import pytest
import synapseclient
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
        {'gene': ['MSN', 'CD44', 'MSN', 'CD44', 'MSN', 'CD44', 'MSN', 'CD44', 'CD44'],
         "score": [1, 2, 1, 2, 1, 2, 1, 2, 1],
         "related": [['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44'], ['CD44']],
         "presence_in_ad_brain": [True, False, True, False, True, False, True, False, False]
         })

    def test_describe_with_mixed_series(self):

        result = _describe_wrapper(df=self.df_mixed)
        assert isinstance(result, dict) is True

        assert result['gene']['mode'] == 'CD44'
        assert result['score']['mode'] == 1
        assert result['score']['min'] == 1
        assert result['score']['max'] == 2
        assert result['presence_in_ad_brain']['mode'] == False # noqa
        assert result['presence_in_ad_brain']['min'] == False # noqa
        assert result['presence_in_ad_brain']['max'] == True # noqa
