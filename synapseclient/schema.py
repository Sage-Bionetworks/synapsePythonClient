"""
******
Schema
******
"""

import json
import requests
import synapseclient
import synapseclient.utils as utils
from synapseclient.client import Synapse
from synapseclient.exceptions import *

_schemaDefinitions = {}

def _init_schemas(synapse):
    """
    Fetches schemas for the various objects used by Synapse
    And constructs default constructors for each, 
    Which can be accessed via synapseclient.schema.<resourceName>
    
    :param synapse: Since schemas may differ depending on the version of Synapse, 
                    an initialized client is required to perform this operation
    """
    
    if synapse is None:
        raise ValueError("Requires an initialized synapse client")
        
    resources = synapse.restGET('/REST/resources', headers=synapse.headers)
    if 'list' not in resources:
        raise SynapseError("Could not fetch the list of schemas")
        
    # Initialize each of the schemas from the list
    for resource in resources['list']:
        schema = synapse.restGET('/REST/resources/effectiveSchema?resourceId=%s' % resource, headers=synapse.headers)
    
        # Save the schema in memory
        _add_dynamic_method(schema['name'])
        _schemaDefinitions[schema['name']] = schema
        
        
def _add_dynamic_method(name):
    """
    Adds a default constructor method to the schema module
    
    :param name: The name of the method to add
    """
    
    # Each schema constructor calls the default constructor
    def dynamicMethod(**kwargs):
        return _schema_constructor(name, **kwargs)
        
    synapseclient.schema.__setattr__(name, dynamicMethod)
    
    
def _schema_constructor(name, **kwargs):
    """
    
    """
    
    if name not in _schemaDefinitions:
        raise ValueError("Could not find a schema for %s" % name)
    
    schema = _schemaDefinitions[name]
    if 'properties' not in schema:
        raise ValueError("Could not parse schema")
        
    result = {}
    
    # Match the keyword arguments
    topLevelArgs = schema['properties'].keys()
    someArgsUnmatched = False
    for keyword in kwargs.keys():
        if keyword in topLevelArgs:
            result[keyword] = kwargs[keyword]
        else:
            print "WARNING: Unknown property '%s'" % keyword
            someArgsUnmatched = True
        
    # Let the user know about the proper arguments
    if someArgsUnmatched or len(result) == 0:
        print "Valid arguments include: %s" % topLevelArgs
        
    return result
    
def _
