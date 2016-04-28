import synapseclient
from synapseclient.entity import is_container
import os

def walk(syn,synId,newpath=None):
    """
    Traverses through the synId specified returning a tuple (directory path, directory names, file names)

    :param synId:          A synapse ID of a folder or project

    Example:

        walkedPath = synu.walk(syn, "syn1234")

        for dirpath, dirname, filename in walkedPath:
            print(dirpath)
            print(dirname) #All the folders in the directory path
            print(filename) #All the files in the directory path

    """
    starting = syn.get(synId,downloadFile=False)
    if newpath is None and not is_container(starting):
        raise ValueError('Cannot traverse through a file, please give a folder or project synId')
    elif newpath is None:
        dirpath = [(starting.name, synId)]
    else:
        dirpath = [(newpath,synId)]
    dirs = []
    nondirs = []
    results = syn.chunkedQuery('select id, name, nodeType from entity where parentId == "%s"'%synId)
    for i in results:
        if is_container(i):
            dirs.append((i['entity.name'],i['entity.id']))
        else:
            nondirs.append((i['entity.name'],i['entity.id']))
    yield dirpath, dirs, nondirs
    for name in dirs:
        newpath = os.path.join(dirpath[0][0],name[0])
        for x in walk(syn, name[1], newpath):
            yield x


