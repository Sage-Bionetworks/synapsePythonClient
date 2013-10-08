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

def _init_schemas(synapse, selection=None):
    """
    Fetches schemas for the various objects used by Synapse
    And constructs default constructors for each, 
    Which can be accessed via synapseclient.schema.<resourceName>
    
    :param synapse:   Since schemas may differ depending on the version of Synapse, 
                      an initialized client is required to perform this operation
    :param selection: An iterable item with substrings of schemas to load
                      i.e. "File" or "Data" (rather than "org.sagebionetworks.repo.model.FileEntity")
    """
    
    if synapse is None:
        raise ValueError("Requires an initialized synapse client")
        
    resources = synapse.restGET('/REST/resources', headers=synapse.headers)
    if 'list' not in resources:
        raise SynapseError("Could not fetch the list of schemas")
        
    schemasToFetch = resources['list']
    if selection is not None:
        included = set()
        for snippet in selection:
            included.update(filter(lambda x: snippet in x, resources['list']))
        schemasToFetch = included
        
    # Initialize each of the schemas from the list
    for resource in schemasToFetch:
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
    Checks the supplied keyword arguments and prints warnings if necessary
    """
    
    if name not in _schemaDefinitions:
        raise ValueError("Could not find a schema for %s" % name)
    
    schema = _schemaDefinitions[name]
    if 'properties' not in schema:
        raise ValueError("Could not parse schema")
        
    return _check_schema_properties(name, schema['properties'], kwargs)
        
    
def _check_schema_properties(currentScope, expectedProperties, givenDict):
    """
    Handles the properties of an "object"-type schema
    
    :returns: A checked dictionary
    """
    
    result = {}
    
    # Match the keyword arguments
    topLevelArgs = expectedProperties.keys()
    someArgsUnmatched = False
    for keyword in givenDict.keys():
        if keyword in topLevelArgs:
            result[keyword] = _check_schema_property(currentScope, expectedProperties[keyword], givenDict[keyword])
        else:
            print "WARNING: Unknown property %s.%s" % (currentScope, keyword)
            someArgsUnmatched = True
        
    # Let the user know about the proper arguments
    if someArgsUnmatched or len(result) == 0:
        print "Valid properties for %s include: %s" % (currentScope, topLevelArgs)
        
    return result
    
def _check_schema_property(currentScope, subSchema, value):
    """
    Takes a dictionary and calls the appropriate method 
    to check the correctness of the arguments.
    
    :returns: The checked value
    """
    
    subType = subSchema['type']
    if subType == "object":
        return _check_schema_properties(currentScope + '.' + subSchema['name'], subSchema['properties'], value)
        
    if subType == "array":
        returnItems = []
        for i in range(len(value)):
            item = value[i]
            returnItems.append(_check_schema_property(currentScope, subSchema['items'], item))
        return returnItems
        
    if subType == "string":
        return str(value)
        
    if subType == "integer":
        return int(value)
        
    ## TODO: Other types are possible
    return value
    
