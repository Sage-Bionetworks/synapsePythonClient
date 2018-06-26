import base64
import time
from nose.tools import assert_equals
from mock import patch

from synapseclient.credentials.cred_data import SynapseCredentials


class TestSynapseCredentials:
    def setup(self):
        self.api_key = b"I am api key"
        self.api_key_b64 = base64.b64encode(self.api_key).decode()
        self.username = "ahhhhhhhhhhhhhh"
        self.credentials = SynapseCredentials(self.username, self.api_key_b64)

    def test_api_key_property(self):
        # test exposed variable
        assert_equals(self.api_key_b64, self.credentials.api_key)

        # test actual internal representation
        assert_equals(self.api_key, self.credentials._api_key)

    def test_get_signed_headers(self):
        url = "https://www.synapse.org/fake_url"

        # mock the 'time' module so the result is always the same instead of dependent upon current time
        fake_time_string = "It is Wednesday, my dudes"
        with patch.object(time, "strftime", return_value=fake_time_string):
            headers = self.credentials.get_signed_headers(url)
            assert_equals({'signatureTimestamp': fake_time_string,
                           'userId': self.username,
                           'signature': b'018ADVu2o2NUOxgO0gM9bo08Wcw='},
                          headers)

    def test_repr(self):
        assert_equals("SynapseCredentials(username='ahhhhhhhhhhhhhh', api_key_string='SSBhbSBhcGkga2V5')",
                      repr(self.credentials))
