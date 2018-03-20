import base64
from nose.tools import assert_equals

from synapseclient.credentials.cred_data import SynapseCredentials

class TestSynapseCredentials():
    def setup(self):
        self.api_key = b"I am api key"
        self.api_key_b64 = base64.b64encode(self.api_key).decode()
        self.username = "ahhhhhhhhhhhhhh"
        self.credentials = SynapseCredentials(self.username, self.api_key_b64)

    def test_api_key_property(self):
        #test exposed variable
        assert_equals(self.api_key_b64, self.credentials.api_key)

        #test actual internal representation
        assert_equals(self.api_key, self.credentials._api_key)

    def test_get_signed_headers(self):
        url = "https://www.synapse.org/fake_url"

        headers = self.credentials.get_signed_headers(url)
        #TODO: how to test w/out basically reimplementing the functions?

    def test_repr(self):
        assert_equals("SynapseCredentials(username='ahhhhhhhhhhhhhh', api_key_string='SSBhbSBhcGkga2V5')",repr(self.credentials))
