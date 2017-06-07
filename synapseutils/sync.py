import errno
from synapseclient.entity import is_container
from synapseclient.utils import id_of, topolgical_sort
from synapseclient import File
from synapseclient.exceptions import *
import os

# from __future__ import absolute_import
# from __future__ import division
# from __future__ import print_function
# from __future__ import unicode_literals

# import io
# import sys
# import six
# import os
# import synapseclient
# from backports import csv


REQUIRED_FIELDS = ['path', 'parent']
FILE_CONSTRUCTOR_FIELDS  = ['name', 'forceVersion', 'synapseStore', 'contentType']
STORE_FUNCTION_FIELDS =  ['used', 'executed', 'activityName', 'activityDescription']


def syncFromSynapse(syn, entity, path=None, ifcollision='overwrite.local', allFiles = None, followLink=False):
    """Synchronizes all the files in a folder (including subfolders) from Synapse and adds a readme manifest with file metadata.

    :param syn:    A synapse object as obtained with syn = synapseclient.login()

    :param entity:  A Synapse ID, a Synapse Entity object of type folder or project.

    :param path: An optional path where the file hierarchy will be
                 reproduced.  If not specified the files will by default
                 be placed in the synapseCache.

    :param ifcollision:   Determines how to handle file collisions.
                          May be "overwrite.local", "keep.local", or "keep.both".
                          Defaults to "overwrite.local".

    :param followLink:  Determines whether the link returns the target Entity.
                        Defaults to False

    :returns: list of entities (files, tables, links)

    This function will crawl all subfolders of the project/folder
    specified by `entity` and download all files that have not already
    been downloaded.  If there are newer files in Synapse (or a local
    file has been edited outside of the cache) since the last download
    then local the file will be replaced by the new file unless
    ifcollision is changed.

    We will also add a file (SYNAPSE_METEDATA_MANIFEST.tsv in the path
    selected that contains the metadata (annotations, storage location
    and provenance of all downloaded files)

    See also: 
    - :py:func:`synapseutils.sync.syncFromSynapse`

    Example::
    Download and print the paths of all downloaded files::

        entities = syncFromSynapse(syn, "syn1234")
        for f in entities:
            print(f.path)

    """
    #TODO: Generate manifest file 
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
            ent = syn.get(result['entity.id'], downloadLocation = path, ifcollision = ifcollision, followLink=followLink)
            allFiles.append(ent)
    return allFiles


def syncToSynapse(syn, manifest_file, notify_me_interval = 1, dry_run=False):
    """Synchronizes files specified in the manifest file to Synapse

    :param syn:    A synapse object as obtained with syn = synapseclient.login()

    :param manifest_file: A tsv file with file locations and metadata
                          to be pushed to Synapse.  See below for details

    :notify_me_interval: The interval (in hours) that this fucntion will notify by email of progess
    
    Given a file describing all of the uploads uploads the content to
    Synapse and optionally notifies you via Synapse messagging (email)
    at specific intervals, on errors and on completion.


    =================
    Manifest file format
    =================

    The format of the manifest file is a tab delimited file with one
    row per file to upload and columns describing the file.  The bare
    minimum required columns is **path** and **parent** where path is
    the local file path and parent is the synapse Id of the project or
    folder where the file is uploaded to.  In addition to these
    columns you can specify any of the parameters to the File
    constructor (**name**, **synapseStore**, **contentType**) as well
    as parameters to the syn.store command (**used**, **executed**,
    **activityName**, **activityDescription**, **forceVersion**).
    Used and executed can be semi-colon(;) separated lists of synapse
    ids, urls and/or local filepaths of files already stored in
    Synapse (or being stored in Synapse by the manifest).  Any
    additional columns will be added as annotations.

    Required Fields:
    =====  =======                =======
    Field  Meaning                Example
    =====  =======                =======
    path   local file path or URL /path/to/local/file.txt
    parent synapse id             syn1235
    =====  =======                =======
                        
                        
    Common Fields:      
    =====        =======                   =======
    Field        Meaning                   Example
    =====        =======                   =======
    name         name of file in Synapse   Example_file
    forceVersion whether to update version False
    =====        =======                   =======
                        
    Provenance Fields:  
    =====                =======                               =======
    Field                Meaning                               Example
    =====                =======                               =======
    used                 List of items used to generate file   syn1235; /path/to_local/file.txt
    executed             List of items exectued                https://github.org/; /path/to_local/code.py
    activityName         Name of activity in provenance        "Ran normalization"
    activityDescription  Text description on what was done     "Ran algorithm xyx with parameters..."
    =====                =======                                     =======
                        
    Annotations:        
                        
    Any columns that are not in the reserved names described above will be intepreted as annotations of the file
                        
    Other Optional fields:
    =====                =======                                     =======
    Field                Meaning                                     Example
    =====                =======                                     =======
    synapseStore         Boolean describing wheterh to upload files  True
    contentType          content type of file to overload defaults   text/html
    =====                =======                                     =======


    ======================
    Example Manifest file
    ======================

    =====                =======     =======   =======   =======                             =======
    path                 parent      annot1    annot2    used                                executed
    =====                =======     =======   =======   =======                             =======
    /path/file1.txt      syn1243     "bar"     3.1415    "syn124; /path/file2.txt"           "https://github.org/foo/bar"
    /path/file2.txt      syn12433    "baz"     2.71      ""                                  "https://github.org/foo/baz"
    =====                =======     =======   =======   =======                             =======
    """

    synapseclient.table.test_import_pandas()
    import pandas as pd
    
    
    #Read manifest file into pandas dataframe
    df = pd.read_csv(manifest_file, sep='\t')


    #Validate manifest for missing columns
    for field in REQUIRED_FIELDS:
        if field not in df.columns:
            raise ValueError("Manifest must contain a column of %s" %field)

    #Validate that manifest does not have circular references.
    
    #df.mycol.get(myIndex, NaN)
    
#    REQUIRED_FIELDS = ['path', 'parent']
#FILE_CONSTRUCTOR_FIELDS  = ['name', 'forceVersion', 'synapseStore', 'contentType']
#STORE_FUNCTION_FIELDS =  ['used', 'executed', 'activityName', 'activityDescription']


# 

# #Read manifest file
# with io.open('batch_testing/batch_test1.csv', 'rt', encoding='utf-8') as f:
# #with io.open('batch_testing/batch_test_cyclic.csv', 'rt', encoding='utf-8') as f:
#     reader = csv.DictReader(f) 
#     rows = [row for row in reader]
# rows = {row['filepath']: row for row in rows}

# #Verify that the right keys are avaialable for each row.
# #TODO

# #Build and verify graph
# uploadOrder = {}
# for filepath, row in rows.iteritems():
#     uploadOrder[filepath] = sum([row['used'].split(';'), row['executed'].split(';')], [])
# uploadOrder = utils.topolgical_sort(uploadOrder)

# #Verify that all dependencies either are in Synapse, a url or being uploaded
# for filepath, dependency in uploadOrder:
#     dependency = [i for i in dependency if i!='']
#     for f in dependency:
#         if f not in rows.keys(): #If file not being uploaded check if in synapse or url
#             if utils.is_url(f):
#                 continue
#             else: #Verify that it is in Synapse
#                 try:
#                     ent = syn.get(f, downloadFile=False)
#                 except SynapseHTTPError as e:
#                     sys.stderr.write('The file %s referenced in proveanance is not stored in Synapse.\n'%f)
#                     raise e

# #Verify that all paths are available
# #TODO

# #Upload the content and set metadata
# for filepath, dependency in uploadOrder:
#     f = File(filepath, parent=row['parentId'])
#     metadata = rows[filepath]

#     f.annotations = {key: value for key, value in metadata.items() if key not in REQUIRED_FIELDS}
#     usedList = [os.path.expanduser(i) for i in metadata['used'].split(';') if i!='']
#     usedList = [syn.get(target) if
#                 (os.path.isfile(target)
#                  if isinstance(target, six.string_types) else False) else target for
#                 target in usedList]

#     executedList = [os.path.expanduser(i) for i in metadata['executed'].split(';') if i!='']
#     executedList = [syn.get(target) if
#                 (os.path.isfile(target) if
#                  isinstance(target, six.string_types) else False) else target for
#                 target in executedList]
#     f = syn.store(f, used = usedList, executed = executedList)
