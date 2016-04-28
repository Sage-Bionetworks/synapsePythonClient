import synapseclient
from synapseclient.entity import is_container
import os

def walk(syn,synID,newpath=None):
    starting = syn.get(synID,downloadFile=False)
    if newpath is None:
        dirpath = [(starting.name, synID)]
    else:
        dirpath = [(newpath,synID)]
    dirs = []
    nondirs = []
    results = syn.chunkedQuery('select id, name, nodeType from entity where parentId == "%s"'%synID)
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


