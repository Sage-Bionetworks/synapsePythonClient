import synapseclient
from synapseclient.entity import is_container
import os
import json

def _getChildren(syn, parentId, includeTypes=["folder","file","table","link","entityview","dockerrepo"], sortBy="NAME", sortDirection="ASC"):
    entityChildrenRequest = {'parentId':parentId,
                             'includeTypes':includeTypes,
                             'sortBy':sortBy,
                             'sortDirection':sortDirection}
    resultPage = {"nextPageToken":"first"}
    while resultPage.get('nextPageToken') is not None:
        resultPage = syn.restPOST('/entity/children',body =json.dumps(entityChildrenRequest))
        for child in resultPage['page']:
            yield child
        if resultPage.get('nextPageToken') is not None:
            entityChildrenRequest['nextPageToken'] = resultPage['nextPageToken']

def walk(syn, synId, includeTypes=None):
    """
    Traverse through the hierarchy of files and folders stored under the synId. Has the same behavior
    as os.walk()

    :param syn:            A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param synId:          A synapse ID of a folder or project

    :param includeTypes:   Must be a list of entity types (ie. ["folder","file"]) which can be found here:
                           http://docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html

    Example::

        walkedPath = walk(syn, "syn1234")

        for dirpath, dirname, filename in walkedPath:
            print(dirpath)
            print(dirname) #All the folders in the directory path
            print(filename) #All the files in the directory path

    """
    if includeTypes is None:
        includeTypes = synapseclient.entity._entity_types
    elif not all([entityType in synapseclient.entity._entity_types for entityType in includeTypes]):
        raise ValueError("Entity type must be part of this list: %s" % ", ".join(entityTypes))
    return(_helpWalk(syn,synId,includeTypes))

#Helper function to hide the newpath parameter
def _helpWalk(syn,synId,includeTypes,newpath=None):
    starting = syn.get(synId,downloadFile=False)
    #If the first file is not a container, return immediately
    if newpath is None and not is_container(starting):
        return
    elif newpath is None:
        dirpath = (starting.name, synId)
    else:
        dirpath = (newpath,synId)
    dirs = []
    nondirs = []
    results = _getChildren(syn, synId, includeTypes=includeTypes)
    for i in results:
        if is_container(i):
            dirs.append((i['name'],i['id']))
        else:
            nondirs.append((i['name'],i['id']))
    yield dirpath, dirs, nondirs
    for name in dirs:
        newpath = os.path.join(dirpath[0],name[0])
        for x in _helpWalk(syn, name[1], includeTypes, newpath=newpath):
            yield x


