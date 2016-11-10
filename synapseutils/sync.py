import errno
from synapseclient.entity import is_container
from synapseclient.utils import id_of
import os


def syncFromSynapse(syn, entity, path=None, ifcollision='overwrite.local', allFiles = None):
    """Synchronizes all the files in a folder (including subfolders) from Synapse.

    :param syn:    A synapse object as obtained with syn = synapseclient.login()

    :param entity:  A Synapse ID, a Synapse Entity object of type folder or project.

    :param path: An optional path where the file hierarchy will be
                 reproduced.  If not specified the files will by default
                 be placed in the synapseCache.

    :param ifcollision:   Determines how to handle file collisions.
                          May be "overwrite.local", "keep.local", or "keep.both".
                          Defaults to "overwrite.local".


    :returns: list of entities (files, tables, links)

    This function will crawl all subfolders of the project/folder
    specified by `id` and download all files that have not already
    been downloaded.  If there are newer files in Synapse (or a local
    file has been edited outside of the cache) since the last download
    then local the file will be replaced by the new file unless
    ifcollision is changed.

    Example::
    Download and print the paths of all downloaded files::

        entities = syncFromSynapse(syn, "syn1234")
        for f in entities:
            print(f.path)
    """
    if allFiles is None: allFiles = list()
    id = id_of(entity)
    results = syn.chunkedQuery("select id, name, nodeType from entity where entity.parentId=='%s'" %id)
    for result in results:
        if is_container(result):
            if path is not None:  #If we are downloading outside cache create directory.
                new_path = os.path.join(path, result['entity.name'])
                try:
                    os.mkdir(new_path)
                except OSError as err:
                    if err.errno!=errno.EEXIST:
                        raise
                print('making dir', new_path)
            else:
                new_path = None
            syncFromSynapse(syn, result['entity.id'], new_path, ifcollision, allFiles)
        else:
            ent = syn.get(result['entity.id'], downloadLocation = path, ifcollision = ifcollision)
            allFiles.append(ent)
    return allFiles
