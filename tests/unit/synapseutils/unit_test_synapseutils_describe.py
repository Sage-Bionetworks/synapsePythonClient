import pytest
import synapseclient
import pandas as pd
import tempfile
from synapseutils.describe import _open_entity_as_df
from unittest.mock import patch, MagicMock


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
        self.df.to_csv(self.csv_path)
        self.csv_ent = synapseclient.File(name='file.csv',
                                          id=self.id,
                                          path=self.csv_path.name,
                                          parentId='8765')

    @pytest.fixture
    def setup_tsv(self):
        self.df.to_csv(self.tsv_path, sep='\t')
        self.tsv_ent = synapseclient.File(name='file.tsv',
                                          id=self.id,
                                          path=self.tsv_path.name,
                                          parentId='8765')

    def test_open_entity_as_df_csv(self, setup_csv):
        with patch.object(self.syn, "get", return_value=self.csv_ent) as mocked_get_entity:

            assert type(_open_entity_as_df(self.syn, self.id)) == pd.DataFrame
            mocked_get_entity.assert_called_once()

    def test_open_entity_as_df_tsv(self, setup_tsv):
        with patch.object(self.syn, "get", return_value=self.tsv_ent) as mocked_get_entity:

            assert type(_open_entity_as_df(self.syn, self.id)) == pd.DataFrame
            mocked_get_entity.assert_called_once()