from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import errno
from synapseclient.entity import is_container
from synapseclient.utils import id_of, topolgical_sort
from synapseclient import File, table
from synapseclient.exceptions import *
import os
import six
import sys

REQUIRED_FIELDS = ['path', 'parent']
FILE_CONSTRUCTOR_FIELDS  = ['name', 'forceVersion', 'synapseStore', 'contentType']
STORE_FUNCTION_FIELDS =  ['used', 'executed', 'activityName', 'activityDescription']
MAX_RETRIES = 10

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
    #TODO: Generate manifest file     
    return allFiles




def syncToSynapse(syn, manifest_file, dry_run=False):
    """Synchronizes files specified in the manifest file to Synapse

    :param syn:    A synapse object as obtained with syn = synapseclient.login()

    :param manifest_file: A tsv file with file locations and metadata
                          to be pushed to Synapse.  See below for details
    
    :param dry_run: Performs validation without uploading if set to True (default is False)

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
    table.test_import_pandas()
    import pandas as pd

    
    sys.stdout.write('Validation and upload of: %s\n' %manifest_file)    
    #Read manifest file into pandas dataframe
    df = pd.read_csv(manifest_file, sep='\t')
    df = df.fillna('')

    sys.stdout.write('Validating columns of manifest...')
    for field in REQUIRED_FIELDS:
        if field not in df.columns:
            raise ValueError("Manifest must contain a column of %s" %field)
    sys.stdout.write('OK\n')

    sys.stdout.write('Validating that all paths exist...')
    sizes = []
    for f in df.path:
        if not os.path.isfile(f):
            print('\nOne of the files you are trying to upload does not exist.')
            raise IOError('The file %s is not available' %f)
        sizes.append(os.stat(f).st_size)
    sys.stdout.write('OK\n')

    
    sys.stdout.write('Validating provenance...')
    df = _sortAndFixProvenance(syn, df)
    sys.stdout.write('OK\n')

    sys.stdout.write('Validating that parents exist and are containers...')
    parents = set(df.parent)
    for synId in parents:
        try:
            container = syn.get(synId, downloadFile=False)
        except SynapseHTTPError as e:
            sys.stdout.write('\n%s in the parent column is not a valid Synapse Id\n' %synId)
            raise(e)
        if not is_container(container):
            sys.stdout.write('\n%s in the parent column is is not a Folder or Project\n' %synId)
            raise SynapseHTTPError
    sys.stdout.write('OK\n')
        
    #Write output on what is getting pushed and estimated times - send out message.
    sys.stdout.write('='*50+'\n')
    sys.stdout.write('We are about to upload %i files with a total size of %s. ' %(len(df), utils.humanizeBytes(sum(sizes))))
    sys.stdout.write('='*50+'\n')

    sys.stdout.write('Starting upload...\n')
    if dry_run:
        return

    retries = 0
    while retries<MAX_RETRIES:
        try:
            done = _manifest_upload(syn, df)
        except e:
            syn.sendMessage([syn.getUserProfile()['ownerId']],
                            messageSubject = 'Upload of %s' %manifest_file, 
                            messageBody = 'Encountered a temporary Failure during upload.  Will retry %i more times. \n\n Error message was:%s' %(MAX_RETRIES-retries, e))
            retries +=1
        if done:
            syn.sendMessage([syn.getUserProfile()['ownerId']],
                            messageSubject = 'Upload of %s' %manifest_file, 
                            messageBody = 'Uploads have completed!')
            break
            
            
def _manifest_upload(syn, df):
    for i, row in df.iterrows():
        #Todo extract known constructor variables
        kwargs = {key: row[key] for key in FILE_CONSTRUCTOR_FIELDS if key in row }
        entity = File(row['path'], parent=row['parent'], **kwargs)
        entity.annotations = dict(row.drop(FILE_CONSTRUCTOR_FIELDS+STORE_FUNCTION_FIELDS+REQUIRED_FIELDS, errors = 'ignore'))
        
        #Update provenance list again to replace all file references that were uploaded
        row['used'] = [syn.get(target) if
                       (os.path.isfile(target) if isinstance(target, six.string_types) else False) else target for
                       target in row['used']]
        row['executed'] = [syn.get(target) if
                       (os.path.isfile(target) if isinstance(target, six.string_types) else False) else target for
                       target in row['executed']]
        
        kwargs = {key: row[key] for key in STORE_FUNCTION_FIELDS if key in row}
        entity = syn.store(entity, **kwargs)
    return True

def _sortAndFixProvenance(syn, df):
    df = df.set_index('path')
    uploadOrder = {}

    def _checkProvenace(item, path):
        """Determines if provenance item is valid"""
        if item is None:
            return item
        if os.path.isfile(item):   
            if item not in df.index: #If it is a file and it is not being uploaded
                try:
                    bundle = syn._getFromFile(item)
                    return bundle
                except SynapseFileNotFoundError:
                    SynapseProvenanceError(("The provenance record for file: %s is incorrect.\n"
                                            "Specifically %s is not being uploaded and is not in Synapse." % (path, item)))
        elif not utils.is_url(item) and (utils.is_synapse_id(item) is None):
            raise SynapseProvenanceError(("The provenance record for file: %s is incorrect.\n"
                                          "Specifically %s, is neither a valid URL or synapseId.") %(path, item))
        return item
    
    for path, row in df.iterrows():
        used = row['used'].split(';')  if ('used' in row) and (row['used'].strip()!='') else []   #Get None or split if string
        executed = row['executed'].split(';') if ('executed' in row) and (row['executed'].strip()!='') else [] #Get None or split if string
        row['used'] =  [_checkProvenace(item, path) for item in used]
        row['executed'] = [_checkProvenace(item, path) for item in executed]
        
        uploadOrder[path] = row['used'] + row['executed']
    uploadOrder = utils.topolgical_sort(uploadOrder)
    df = df.reindex([l[0] for l in uploadOrder])
    return df.reset_index()
    

