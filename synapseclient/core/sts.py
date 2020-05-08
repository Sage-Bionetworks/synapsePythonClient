from synapseclient.core.utils import snake_case


def get_sts_credentials(syn, entity_id, read_only: bool):
    permission = "read_only" if read_only else "read_write"
    response = syn.restGET(f'/entity/{entity_id}/sts?permission={permission}')

    # the Synapse STS API returns camel cased keys that we need to convert to use with boto.
    # prefix with "aws_", convert to snake case, and exclude any other key/value pairs in the response
    # e.g. expiration
    return {"aws_{}".format(snake_case(k)): response[k] for k in (
        'accessKeyId', 'secretAccessKey', 'sessionToken'
    )}
