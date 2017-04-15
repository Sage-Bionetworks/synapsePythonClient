import synapseclient
from synapseclient.entity import is_container
import os
import json

def _getChildren(syn, parentId, includeTypes=["folder","file","table","link","entityview","dockerrepo"], sortBy="NAME", sortDirection="ASC", nextPageToken=None):
    entityChildrenRequest = {'parentId':parentId,
                             'includeTypes':includeTypes,
                             'sortBy':sortBy,
                             'sortDirection':sortDirection,
                             'nextPageToken':nextPageToken}

    resultPage = syn.restPOST('/entity/children',body =json.dumps(entityChildrenRequest))
    for result in resultPage['page']:
        yield result
    if resultPage.get('nextPageToken') is not None:
        for x in _getChildren(syn, parentId, includeTypes=["folder","file","table","link","entityview","dockerrepo"], sortBy="NAME", sortDirection="ASC", nextPageToken=resultPage.get('nextPageToken')):
            yield x

def walk(syn, synId):
    """
    Traverse through the hierarchy of files and folders stored under the synId. Has the same behavior
    as os.walk()

    :param syn:            A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param synId:          A synapse ID of a folder or project

    Example::

        walkedPath = walk(syn, "syn1234")

        for dirpath, dirname, filename in walkedPath:
            print(dirpath)
            print(dirname) #All the folders in the directory path
            print(filename) #All the files in the directory path

    """
    return(_helpWalk(syn,synId))

#Helper function to hide the newpath parameter
def _helpWalk(syn,synId,newpath=None):
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
    results = _getChildren(syn, synId)
    for i in results:
        if i['type'] in (synapseclient.Project._synapse_entity_type, synapseclient.Folder._synapse_entity_type):
            dirs.append((i['name'],i['id']))
        else:
            nondirs.append((i['name'],i['id']))
    yield dirpath, dirs, nondirs
    for name in dirs:
        newpath = os.path.join(dirpath[0],name[0])
        for x in _helpWalk(syn, name[1], newpath):
            yield x


