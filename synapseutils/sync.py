import csv
import errno
from .monitor import notifyMe
from synapseclient.entity import is_container
from synapseclient.core.utils import id_of, is_url, is_synapse_id
from synapseclient import File, table
from synapseclient.core.exceptions import *
import os
import io
import sys

REQUIRED_FIELDS = ['path', 'parent']
FILE_CONSTRUCTOR_FIELDS = ['name', 'synapseStore', 'contentType']
STORE_FUNCTION_FIELDS = ['used', 'executed', 'activityName', 'activityDescription', 'forceVersion']
MAX_RETRIES = 4
MANIFEST_FILENAME = 'SYNAPSE_METADATA_MANIFEST.tsv'
DEFAULT_GENERATED_MANIFEST_KEYS = ['path', 'parent', 'name', 'synapseStore', 'contentType', 'used', 'executed',
                                   'activityName', 'activityDescription']


def syncFromSynapse(syn, entity, path=None, ifcollision='overwrite.local', allFiles=None, followLink=False):
    """Synchronizes all the files in a folder (including subfolders) from Synapse and adds a readme manifest with file
    metadata.

    :param syn:         A synapse object as obtained with syn = synapseclient.login()

    :param entity:      A Synapse ID, a Synapse Entity object of type file, folder or project.

    :param path:        An optional path where the file hierarchy will be reproduced. If not specified the files will by
                        default be placed in the synapseCache.

    :param ifcollision: Determines how to handle file collisions. Maybe "overwrite.local", "keep.local", or "keep.both".
                        Defaults to "overwrite.local".

    :param followLink:  Determines whether the link returns the target Entity.
                        Defaults to False

    :returns: list of entities (files, tables, links)

    This function will crawl all subfolders of the project/folder specified by `entity` and download all files that have
    not already been downloaded.  If there are newer files in Synapse (or a local file has been edited outside of the
    cache) since the last download then local the file will be replaced by the new file unless "ifcollision" is changed.

    If the files are being downloaded to a specific location outside of the Synapse cache a file
    (SYNAPSE_METADATA_MANIFEST.tsv) will also be added in the path that contains the metadata (annotations, storage
    location and provenance of all downloaded files).

    See also:
    - :py:func:`synapseutils.sync.syncToSynapse`

    Example:
    Download and print the paths of all downloaded files::

        entities = syncFromSynapse(syn, "syn1234")
        for f in entities:
            print(f.path)

    """
    # initialize the result list
    if allFiles is None:
        allFiles = list()

    # perform validation check on user input
    if is_synapse_id(entity):
        entity = syn.get(entity, downloadLocation=path, ifcollision=ifcollision, followLink=followLink)

    if isinstance(entity, File):
        allFiles.append(entity)
        return allFiles

    entity_id = id_of(entity)
    if not is_container(entity):
        raise ValueError("The provided id: %s is neither a container nor a File" % entity_id)

    # get the immediate children as iterator
    children = syn.getChildren(entity_id)

    # process each child
    for child in children:
        if is_container(child):
            # If we are downloading outside cache create directory
            if path is not None:
                new_path = os.path.join(path, child['name'])
                try:
                    os.makedirs(new_path)
                except OSError as err:
                    if err.errno != errno.EEXIST:
                        raise
            else:
                new_path = None
            # recursively explore this container's children
            syncFromSynapse(syn, child['id'], new_path, ifcollision, allFiles, followLink=followLink)
        else:
            # getting the child
            ent = syn.get(child['id'], downloadLocation=path, ifcollision=ifcollision, followLink=followLink)
            if isinstance(ent, File):
                allFiles.append(ent)

    if path is not None:  # If path is None files are stored in cache.
        filename = os.path.join(path, MANIFEST_FILENAME)
        filename = os.path.expanduser(os.path.normcase(filename))
        generateManifest(syn, allFiles, filename)

    return allFiles


def generateManifest(syn, allFiles, filename):
    """Generates a manifest file based on a list of entities objects.

    :param allFiles:   A list of File Entities

    :param filename: file where manifest will be written
    """
    keys, data = _extract_file_entity_metadata(syn, allFiles)
    _write_manifest_data(filename, keys, data)


def _extract_file_entity_metadata(syn, allFiles):
    """
    Extracts metadata from the list of File Entities and returns them in a form usable by csv.DictWriter
    :param syn:         instance of the Synapse client
    :param allFiles:    an iterable that provides File entities

    :return: (keys: a list column headers, data: a list of dicts containing data from each row)
    """
    keys = list(DEFAULT_GENERATED_MANIFEST_KEYS)
    annotKeys = set()
    data = []
    for entity in allFiles:
        row = {'parent': entity['parentId'], 'path': entity.get("path"), 'name': entity.name,
               'synapseStore': entity.synapseStore, 'contentType': entity['contentType']}
        row.update({key: (val[0] if len(val) > 0 else "") for key, val in entity.annotations.items()})

        row.update(_get_file_entity_provenance_dict(syn, entity))

        annotKeys.update(set(entity.annotations.keys()))

        data.append(row)
    keys.extend(annotKeys)
    return keys, data


def _get_file_entity_provenance_dict(syn, entity):
    """
    Returns a dict with a subset of the provenance metadata for the entity.
    An empty dict is returned if the metadata does not have a provenance record.
    """
    try:
        prov = syn.getProvenance(entity)
        return {'used': ';'.join(prov._getUsedStringList()),
                'executed': ';'.join(prov._getExecutedStringList()),
                'activityName': prov.get('name', ''),
                'activityDescription': prov.get('description', '')}
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            return {}  # No provenance present return empty dict
        else:
            raise  # unexpected error so we re-raise the exception


def _write_manifest_data(filename, keys, data):
    with io.open(filename, 'w', encoding='utf8') as fp:
        csvWriter = csv.DictWriter(fp, keys, restval='', extrasaction='ignore', delimiter='\t')
        csvWriter.writeheader()
        for row in data:
            csvWriter.writerow(row)


def _sortAndFixProvenance(syn, df):
    df = df.set_index('path')
    uploadOrder = {}

    def _checkProvenace(item, path):
        """Determines if provenance item is valid"""
        if item is None:
            return item

        item_path_normalized = os.path.abspath(os.path.expandvars(os.path.expanduser(item)))
        if os.path.isfile(item_path_normalized):
            # Add full path
            item = item_path_normalized
            if item not in df.index:  # If it is a file and it is not being uploaded
                try:
                    bundle = syn._getFromFile(item)
                    return bundle
                except SynapseFileNotFoundError:
                    SynapseProvenanceError(("The provenance record for file: %s is incorrect.\n"
                                            "Specifically %s is not being uploaded and is not in Synapse."
                                            % (path, item)))

        elif not utils.is_url(item) and (utils.is_synapse_id(item) is None):
            raise SynapseProvenanceError(("The provenance record for file: %s is incorrect.\n"
                                          "Specifically %s, is neither a valid URL or synapseId.") % (path, item))
        return item

    for path, row in df.iterrows():
        allRefs = []
        if 'used' in row:
            used = row['used'].split(';') if (row['used'].strip() != '') else []  # Get None or split if string
            df.at[path, 'used']=[_checkProvenace(item, path) for item in used]
            allRefs.extend(df.loc[path, 'used'])
        if 'executed' in row:
            # Get None or split if string
            executed = row['executed'].split(';') if (row['executed'].strip() != '') else []
            df.at[path, 'executed'] = [_checkProvenace(item, path) for item in executed]
            allRefs.extend(df.loc[path, 'executed'])
        uploadOrder[path] = allRefs

    uploadOrder = utils.topolgical_sort(uploadOrder)
    df = df.reindex([l[0] for l in uploadOrder])
    return df.reset_index()


def _check_path_and_normalize(f):
    sys.stdout.write('.')
    if is_url(f):
        return f
    path_normalized = os.path.abspath(os.path.expandvars(os.path.expanduser(f)))
    if not os.path.isfile(path_normalized):
        print('\nThe specified path "%s" is either not a file path or does not exist.', f)
        raise IOError('The path %s is not a file or does not exist' % f)
    return path_normalized


def readManifestFile(syn, manifestFile):
    """Verifies a file manifest and returns a reordered dataframe ready for upload.

    :param syn:             A synapse object as obtained with syn = synapseclient.login()

    :param manifestFile:    A tsv file with file locations and metadata to be pushed to Synapse.
                            See below for details

    :returns A pandas dataframe if the manifest is validated.

    See also for a description of the file format:
        - :py:func:`synapseutils.sync.syncToSynapse`
    """
    table.test_import_pandas()
    import pandas as pd

    sys.stdout.write('Validation and upload of: %s\n' % manifestFile)
    # Read manifest file into pandas dataframe
    df = pd.read_csv(manifestFile, sep='\t')
    if 'synapseStore' not in df:
        df = df.assign(synapseStore=None)
    df.loc[df['path'].apply(is_url), 'synapseStore'] = False  # override synapseStore values to False when path is a url
    df.loc[df['synapseStore'].isnull(), 'synapseStore'] = True  # remaining unset values default to True
    df.synapseStore = df.synapseStore.astype(bool)
    df = df.fillna('')

    sys.stdout.write('Validating columns of manifest...')
    for field in REQUIRED_FIELDS:
        sys.stdout.write('.')
        if field not in df.columns:
            sys.stdout.write('\n')
            raise ValueError("Manifest must contain a column of %s" % field)
    sys.stdout.write('OK\n')

    sys.stdout.write('Validating that all paths exist')
    df.path = df.path.apply(_check_path_and_normalize)

    sys.stdout.write('OK\n')

    sys.stdout.write('Validating that all files are unique...')
    if len(df.path) != len(set(df.path)):
        raise ValueError("All rows in manifest must contain a unique file to upload")
    sys.stdout.write('OK\n')

    sys.stdout.write('Validating provenance...')
    df = _sortAndFixProvenance(syn, df)
    sys.stdout.write('OK\n')

    sys.stdout.write('Validating that parents exist and are containers...')
    parents = set(df.parent)
    for synId in parents:
        try:
            container = syn.get(synId, downloadFile=False)
        except SynapseHTTPError:
            sys.stdout.write('\n%s in the parent column is not a valid Synapse Id\n' % synId)
            raise
        if not is_container(container):
            sys.stdout.write('\n%s in the parent column is is not a Folder or Project\n' % synId)
            raise SynapseHTTPError
    sys.stdout.write('OK\n')
    return df


def syncToSynapse(syn, manifestFile, dryRun=False, sendMessages=True, retries=MAX_RETRIES):
    """Synchronizes files specified in the manifest file to Synapse

    :param syn:             A synapse object as obtained with syn = synapseclient.login()

    :param manifestFile:    A tsv file with file locations and metadata to be pushed to Synapse.
                            See below for details

    :param dryRun: Performs validation without uploading if set to True (default is False)

    Given a file describing all of the uploads uploads the content to Synapse and optionally notifies you via Synapse
    messagging (email) at specific intervals, on errors and on completion.

    **Manifest file format**

    The format of the manifest file is a tab delimited file with one row per file to upload and columns describing the
    file. The minimum required columns are **path** and **parent** where path is the local file path and parent is the
    Synapse Id of the project or folder where the file is uploaded to. In addition to these columns you can specify any
    of the parameters to the File constructor (**name**, **synapseStore**, **contentType**) as well as parameters to the
    syn.store command (**used**, **executed**, **activityName**, **activityDescription**, **forceVersion**).
    Used and executed can be semi-colon (";") separated lists of Synapse ids, urls and/or local filepaths of files
    already stored in Synapse (or being stored in Synapse by the manifest).
    Any additional columns will be added as annotations.

    **Required fields:**

    ======   ======================                  ============================
    Field    Meaning                                 Example
    ======   ======================                  ============================
    path     local file path or URL                  /path/to/local/file.txt
    parent   synapse id                              syn1235
    ======   ======================                  ============================
                        
                        
    **Common fields:**
    
    ===============        ===========================                   ============
    Field                  Meaning                                       Example
    ===============        ===========================                   ============
    name                   name of file in Synapse                       Example_file
    forceVersion           whether to update version                     False
    ===============        ===========================                   ============
                        
    **Provenance fields:**  

    ====================   =====================================  ==========================================
    Field                  Meaning                                Example
    ====================   =====================================  ==========================================
    used                   List of items used to generate file    syn1235; /path/to_local/file.txt
    executed               List of items exectued                 https://github.org/; /path/to_local/code.py
    activityName           Name of activity in provenance         "Ran normalization"
    activityDescription    Text description on what was done      "Ran algorithm xyx with parameters..."
    ====================   =====================================  ==========================================

    Annotations:

    **Annotations:**
                        
    Any columns that are not in the reserved names described above will be interpreted as annotations of the file
                        
    **Other optional fields:**

    ===============          ==========================================  ============
    Field                    Meaning                                     Example
    ===============          ==========================================  ============
    synapseStore             Boolean describing whether to upload files  True
    contentType              content type of file to overload defaults   text/html
    ===============          ==========================================  ============


    **Example manifest file**

    ===============   ========    =======   =======   ===========================    ============================
    path              parent      annot1    annot2    used                           executed
    ===============   ========    =======   =======   ===========================    ============================
    /path/file1.txt   syn1243     "bar"     3.1415    "syn124; /path/file2.txt"      "https://github.org/foo/bar"
    /path/file2.txt   syn12433    "baz"     2.71      ""                             "https://github.org/foo/baz"
    ===============   ========    =======   =======   ===========================    ============================

    """
    df = readManifestFile(syn, manifestFile)
    sizes = [os.stat(os.path.expandvars(os.path.expanduser(f))).st_size for f in df.path if not is_url(f)]
    # Write output on what is getting pushed and estimated times - send out message.
    sys.stdout.write('='*50+'\n')
    sys.stdout.write('We are about to upload %i files with a total size of %s.\n '
                     % (len(df), utils.humanizeBytes(sum(sizes))))
    sys.stdout.write('='*50+'\n')

    if dryRun:
        return

    sys.stdout.write('Starting upload...\n')
    if sendMessages:
        notify_decorator = notifyMe(syn, 'Upload of %s' % manifestFile, retries=retries)
        upload = notify_decorator(_manifest_upload)
        upload(syn, df)
    else:
        _manifest_upload(syn, df)


def _manifest_upload(syn, df):
    for i, row in df.iterrows():
        # TODO: extract known constructor variables
        kwargs = {key: row[key] for key in FILE_CONSTRUCTOR_FIELDS if key in row}
        entity = File(row['path'], parent=row['parent'], **kwargs)
        entity.annotations = dict(row.drop(FILE_CONSTRUCTOR_FIELDS + STORE_FUNCTION_FIELDS+REQUIRED_FIELDS,
                                           errors='ignore'))

        # Update provenance list again to replace all file references that were uploaded
        if 'used' in row:
            row['used'] = syn._convertProvenanceList(row['used'])
        if 'executed' in row:
            row['executed'] = syn._convertProvenanceList(row['executed'])
        kwargs = {key: row[key] for key in STORE_FUNCTION_FIELDS if key in row}
        syn.store(entity, **kwargs)
    return True
