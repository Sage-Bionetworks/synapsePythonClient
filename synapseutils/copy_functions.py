import synapseclient
from synapseclient import File, Project, Folder, Table, Schema, Link, Wiki, Entity, Activity
from synapseclient.core.cache import Cache
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.constants.limits import *
import re
import json
import itertools
import math

############################################################
#                  Copy Functions                          #
############################################################


def copyFileHandles(syn, fileHandles, associateObjectTypes, associateObjectIds,
                    newContentTypes=None, newFileNames=None):
    """
    Given a list of fileHandle Ids or Objects, copy the fileHandles

    :param syn:                     A Synapse object with user's login, e.g. syn = synapseclient.login()

    :param fileHandles:             List of fileHandle Ids or Objects

    :param associateObjectTypes:    List of associated object types: FileEntity, TableEntity, WikiAttachment,
                                    UserProfileAttachment, MessageAttachment, TeamAttachment, SubmissionAttachment,
                                    VerificationSubmission (Must be the same length as fileHandles)

    :param associateObjectIds:      List of associated object Ids: If copying a file, the objectId is the synapse id,
                                    and if copying a wiki attachment, the object id is the wiki subpage id.
                                    (Must be the same length as fileHandles)

    :param newContentTypes:         (Optional) List of content types. Set each item to a new content type for each file handle, or leave the item as None to keep the original content type. Default None, which keeps all original content types.

    :param newFileNames:            (Optional) List of filenames. Set each item to a new filename for each file handle, or leave the item as None to keep the original name. Default None, which keeps all original file names.

    :return:                        List of batch filehandle copy results, can include failureCodes: UNAUTHORIZED and
                                    NOT_FOUND

    :raises ValueError: If length of all input arguments are not the same
    """

    # Check if length of all inputs are equal
    if not (len(fileHandles) == len(associateObjectTypes) == len(associateObjectIds)
            and (newContentTypes is None or len(newContentTypes) == len(associateObjectIds))
            and (newFileNames is None or len(newFileNames) == len(associateObjectIds))):
        raise ValueError("Length of all input arguments must be the same")

    # If no optional params passed, assign to empty list
    if newContentTypes is None:
        newContentTypes = []
    if newFileNames is None:
        newFileNames = []

    # Remove this line if we change API to only take fileHandleIds and not Objects
    file_handle_ids = [synapseclient.core.utils.id_of(handle) for handle in fileHandles]

    # division logic for POST call here
    master_copy_results_list = []  # list which holds all results from POST call
    for batch_file_handles_ids, batch_assoc_obj_types, batch_assoc_obj_ids, batch_con_type, batch_file_name \
            in _batch_iterator_generator([file_handle_ids, associateObjectTypes, associateObjectIds,
                                          newContentTypes, newFileNames], MAX_FILE_HANDLE_PER_COPY_REQUEST):

        batch_copy_results = _copy_file_handles_batch(syn, batch_file_handles_ids, batch_assoc_obj_types,
                                                      batch_assoc_obj_ids, batch_con_type, batch_file_name)
        master_copy_results_list.extend(batch_copy_results)

    return master_copy_results_list


def _copy_file_handles_batch(self, file_handle_ids, obj_types, obj_ids, new_con_types, new_file_names):
    """
    Given a list of fileHandle Ids, copy the fileHandles. This helper makes the POST call and returns the
    results as a list.

    :param self:                    A Synapse object with user's login, e.g. syn = synapseclient.login()

    :param file_handle_ids:         List of fileHandle Ids or Objects

    :param obj_types:               List of associated object types: FileEntity, TableEntity, WikiAttachment,
                                    UserProfileAttachment, MessageAttachment, TeamAttachment, SubmissionAttachment,
                                    VerificationSubmission (Must be the same length as fileHandles)

    :param obj_ids:                 List of associated object Ids: If copying a file, the objectId is the synapse id,
                                    and if copying a wiki attachment, the object id is the wiki subpage id.
                                    (Must be the same length as fileHandles)

    :param new_con_types:           (Optional) List of content types (Can change a filetype of a filehandle).

    :param new_file_names:          (Optional) List of filenames (Can change a filename of a filehandle).

    :return:                        List of batch filehandle copy results, can include failureCodes: UNAUTHORIZED and
                                    NOT_FOUND
    """
    copy_file_handle_request = _create_batch_file_handle_copy_request(file_handle_ids, obj_types, obj_ids,
                                                                      new_con_types, new_file_names)
    # make backend call which performs the copy specified by copy_file_handle_request
    copied_file_handles = self.restPOST('/filehandles/copy', body=json.dumps(copy_file_handle_request),
                                        endpoint=self.fileHandleEndpoint)
    return copied_file_handles.get("copyResults")


def _create_batch_file_handle_copy_request(file_handle_ids, obj_types, obj_ids, new_con_types, new_file_names):
    """
    Returns json for file handle copy request

    :param file_handle_ids:         List of fileHandle Ids

    :param obj_types:               List of associated object types: FileEntity, TableEntity, WikiAttachment,
                                    UserProfileAttachment, MessageAttachment, TeamAttachment, SubmissionAttachment,
                                    VerificationSubmission (Must be the same length as fileHandles)

    :param obj_ids:                 List of associated object Ids: If copying a file, the objectId is the synapse id,
                                    and if copying a wiki attachment, the object id is the wiki subpage id.
                                    (Must be the same length as fileHandles)

    :param new_con_types:           List of content types (Can change a filetype of a filehandle).

    :param new_file_names:          List of filenames (Can change a filename of a filehandle).

    :return:                        JSON for API call to POST/ filehandles/ copy
    """
    copy_file_handle_request = {"copyRequests": []}
    for file_handle_id, obj_type, obj_id, new_con_type, new_file_name \
            in itertools.zip_longest(file_handle_ids, obj_types, obj_ids, new_con_types, new_file_names):
        # construct JSON object for REST call
        curr_dict = {
            "originalFile": {
                "fileHandleId": file_handle_id,
                "associateObjectId": obj_id,
                "associateObjectType": obj_type
            },
            "newContentType": new_con_type,
            "newFileName": new_file_name
        }

        # add copy request to list of requests
        copy_file_handle_request["copyRequests"].append(curr_dict)
    return copy_file_handle_request


def _batch_iterator_generator(iterables, batch_size):
    """
    Returns a generator over each of the iterable objects in the list iterables
    :param iterables: List of iterable objects, all must be same length, len(iterables) >= 1
    :param batch_size: Integer representing the size of the batch, batch_size >= 1
    :return: Generator which yields a list of batches for each iterable in iterables
    :raises ValueError: If len(iterables) < 1

    example: _batch_iterator_generator(["ABCDEFG"], 3) --> ["ABC"] ["DEF"] ["G"], on successive calls to next()
             _batch_iterator_generator([[1, 2, 3], [4, 5, 6]], 2) --> [[1, 2], [4, 5]] [[3], [6]]
    """
    if len(iterables) < 1:
        raise ValueError("Must provide at least one iterable in iterables, i.e. len(iterables) >= 1")

    num_batches = math.ceil(len(iterables[0]) / batch_size)
    for i in range(num_batches):
        start = i * batch_size
        end = start + batch_size
        yield [iterables[i][start:end] for i in range(len(iterables))]


def _copy_cached_file_handles(cache, copiedFileHandles):
    # type: (Cache , dict) -> None
    for copy_result in copiedFileHandles:
        if copy_result.get('failureCode') is None:  # sucessfully copied
            original_cache_path = cache.get(copy_result['originalFileHandleId'])
            if original_cache_path:
                cache.add(copy_result['newFileHandle']['id'], original_cache_path)


def changeFileMetaData(syn, entity, downloadAs=None, contentType=None):
    """
    :param entity:        Synapse entity Id or object

    :param contentType:   Specify content type to change the content type of a filehandle

    :param downloadAs:    Specify filename to change the filename of a filehandle

    :return:              Synapse Entity

    Can be used to change the filename or the file content-type without downloading::

        file_entity = syn.get(synid)
        print(os.path.basename(file_entity.path))  ## prints, e.g., "my_file.txt"
        file_entity = synapseutils.changeFileMetaData(syn, file_entity, "my_new_name_file.txt")
    """
    ent = syn.get(entity, downloadFile=False)
    fileResult = syn._getFileHandleDownload(ent.dataFileHandleId, ent.id)
    ent.contentType = ent.contentType if contentType is None else contentType
    downloadAs = fileResult['fileHandle']['fileName'] if downloadAs is None else downloadAs
    copiedFileHandle = copyFileHandles(syn, [ent.dataFileHandleId], [ent.concreteType.split(".")[-1]], [ent.id],
                                       [contentType], [downloadAs])
    copyResult = copiedFileHandle[0]
    if copyResult.get("failureCode") is not None:
        raise ValueError("%s dataFileHandleId: %s" % (copyResult["failureCode"], copyResult['originalFileHandleId']))
    ent.dataFileHandleId = copyResult['newFileHandle']['id']
    ent = syn.store(ent)
    return ent

def copy(syn, entity, destinationId, skipCopyWikiPage=False, skipCopyAnnotations=False, **kwargs):
    """
    - This function will assist users in copying entities (Tables, Links, Files, Folders, Projects),
      and will recursively copy everything in directories.
    - A Mapping of the old entities to the new entities will be created and all the wikis of each entity
      will also be copied over and links to synapse Ids will be updated.

    :param syn:                 A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param entity:              A synapse entity ID

    :param destinationId:       Synapse ID of a folder/project that the copied entity is being copied to

    :param skipCopyWikiPage:    Skip copying the wiki pages
                                Default is False

    :param skipCopyAnnotations: Skips copying the annotations
                                Default is False

    Examples::                        
    import synapseutils
    import synapseclient
    syn = synapseclient.login()
    synapseutils.copy(syn, ...)

    Examples and extra parameters unique to each copy function
    -- COPYING FILES

    :param version:         Can specify version of a file. 
                            Default to None

    :param updateExisting:  When the destination has an entity that has the same name, 
                            users can choose to update that entity.  
                            It must be the same entity type
                            Default to False
    
    :param setProvenance:   Has three values to set the provenance of the copied entity:
                            traceback: Sets to the source entity
                            existing: Sets to source entity's original provenance (if it exists)
                            None: No provenance is set

    Examples::
        synapseutils.copy(syn, "syn12345", "syn45678", updateExisting=False, setProvenance = "traceback",version=None)

    -- COPYING FOLDERS/PROJECTS

    :param excludeTypes:    Accepts a list of entity types (file, table, link) which determines which entity types to
                            not copy.
                            Defaults to an empty list.

    Examples::
    #This will copy everything in the project into the destinationId except files and tables.
    synapseutils.copy(syn, "syn123450","syn345678",excludeTypes=["file","table"])

    :returns: a mapping between the original and copied entity: {'syn1234':'syn33455'}
    """
    updateLinks = kwargs.get('updateLinks', True)
    updateSynIds = kwargs.get('updateSynIds', True)
    entitySubPageId = kwargs.get('entitySubPageId', None)
    destinationSubPageId = kwargs.get('destinationSubPageId', None)

    mapping = _copyRecursive(syn, entity, destinationId, skipCopyAnnotations=skipCopyAnnotations, **kwargs)
    if not skipCopyWikiPage:
        for oldEnt in mapping:
            copyWiki(syn, oldEnt, mapping[oldEnt], entitySubPageId=entitySubPageId,
                     destinationSubPageId=destinationSubPageId, updateLinks=updateLinks,
                     updateSynIds=updateSynIds, entityMap=mapping)
    return mapping


def _copyRecursive(syn, entity, destinationId, mapping=None, skipCopyAnnotations=False, **kwargs):
    """
    Recursively copies synapse entites, but does not copy the wikis

    :param entity:              A synapse entity ID

    :param destinationId:       Synapse ID of a folder/project that the copied entity is being copied to
    
    :param skipCopyAnnotations: Skips copying the annotations
                                Default is False

    :returns: a mapping between the original and copied entity: {'syn1234':'syn33455'}
    """

    version = kwargs.get('version', None)
    setProvenance = kwargs.get('setProvenance', "traceback")
    excludeTypes = kwargs.get('excludeTypes', [])
    updateExisting = kwargs.get('updateExisting', False)
    if mapping is None:
        mapping = dict()
    # Check that passed in excludeTypes is file, table, and link
    if not isinstance(excludeTypes, list):
        raise ValueError("Excluded types must be a list") 
    elif not all([i in ["file", "link", "table"] for i in excludeTypes]):
        raise ValueError("Excluded types can only be a list of these values: file, table, and link") 

    ent = syn.get(entity, downloadFile=False)
    if ent.id == destinationId:
        raise ValueError("destinationId cannot be the same as entity id")

    if (isinstance(ent, Project) or isinstance(ent, Folder)) and version is not None:
        raise ValueError("Cannot specify version when copying a project of folder")

    if not isinstance(ent, (Project, Folder, File, Link, Schema, Entity)):
        raise ValueError("Not able to copy this type of file")

    permissions = syn.restGET("/entity/{}/permissions".format(ent.id))
    # Don't copy entities without DOWNLOAD permissions
    if not permissions['canDownload']:
        print("%s not copied - this file lacks download permission" % ent.id)
        return mapping

    access_requirements = syn.restGET('/entity/{}/accessRequirement'.format(ent.id))
    # If there are any access requirements, don't copy files
    if access_requirements['results']:
        print("{} not copied - this file has access restrictions".format(ent.id))
        return mapping
    copiedId = None

    if isinstance(ent, Project):
        if not isinstance(syn.get(destinationId), Project):
            raise ValueError("You must give a destinationId of a new project to copy projects")
        copiedId = destinationId
        # Projects include Docker repos, and Docker repos cannot be copied
        # with the Synapse rest API. Entity views currently also aren't
        # supported
        entities = syn.getChildren(entity, includeTypes=['folder', 'file',
                                                         'table', 'link'])
        for i in entities:
            mapping = _copyRecursive(syn, i['id'], destinationId, mapping=mapping,
                                     skipCopyAnnotations=skipCopyAnnotations, **kwargs)
    elif isinstance(ent, Folder):
        copiedId = _copyFolder(syn, ent.id, destinationId, mapping=mapping, skipCopyAnnotations=skipCopyAnnotations,
                               **kwargs)
    elif isinstance(ent, File) and "file" not in excludeTypes:
        copiedId = _copyFile(syn, ent.id, destinationId, version=version, updateExisting=updateExisting,
                             setProvenance=setProvenance, skipCopyAnnotations=skipCopyAnnotations)
    elif isinstance(ent, Link) and "link" not in excludeTypes:
        copiedId = _copyLink(syn, ent.id, destinationId, updateExisting=updateExisting)
    elif isinstance(ent, Schema) and "table" not in excludeTypes:
        copiedId = _copyTable(syn, ent.id, destinationId, updateExisting=updateExisting)
    # This is currently done because copyLink returns None sometimes
    if copiedId is not None:
        mapping[ent.id] = copiedId
        print("Copied %s to %s" % (ent.id, copiedId))
    else:
        print("%s not copied" % ent.id)
    return mapping


def _copyFolder(syn, entity, destinationId, mapping=None, skipCopyAnnotations=False, **kwargs):
    """
    Copies synapse folders

    :param entity:              A synapse ID of a Folder entity

    :param destinationId:       Synapse ID of a project/folder that the folder wants to be copied to
    
    :param skipCopyAnnotations: Skips copying the annotations
                                Default is False
    """
    oldFolder = syn.get(entity)
    updateExisting = kwargs.get('updateExisting', False)

    if mapping is None:
        mapping = dict()
    # CHECK: If Folder name already exists, raise value error
    if not updateExisting:
        existingEntity = syn.findEntityId(oldFolder.name, parent=destinationId)
        if existingEntity is not None:
            raise ValueError('An entity named "%s" already exists in this location. Folder could not be copied'
                             % oldFolder.name)

    newFolder = Folder(name=oldFolder.name, parent=destinationId)
    if not skipCopyAnnotations:
        newFolder.annotations = oldFolder.annotations
    newFolder = syn.store(newFolder)
    entities = syn.getChildren(entity)
    for ent in entities:
        _copyRecursive(syn, ent['id'], newFolder.id, mapping, skipCopyAnnotations=skipCopyAnnotations, **kwargs)
    return newFolder.id


def _copyFile(syn, entity, destinationId, version=None, updateExisting=False, setProvenance="traceback",
              skipCopyAnnotations=False):
    """
    Copies most recent version of a file to a specified synapse ID.

    :param entity:              A synapse ID of a File entity

    :param destinationId:       Synapse ID of a folder/project that the file wants to be copied to

    :param version:             Can specify version of a file. 
                                Default to None

    :param updateExisting:      Can choose to update files that have the same name 
                                Default to False
    
    :param setProvenance:       Has three values to set the provenance of the copied entity:
                                    traceback: Sets to the source entity
                                    existing: Sets to source entity's original provenance (if it exists)
                                    None: No provenance is set
    :param skipCopyAnnotations: Skips copying the annotations
                                Default is False
    """
    ent = syn.get(entity, downloadFile=False, version=version, followLink=False)
    # CHECK: If File is in the same parent directory (throw an error) (Can choose to update files)
    if not updateExisting:
        existingEntity = syn.findEntityId(ent.name, parent=destinationId)
        if existingEntity is not None:
            raise ValueError('An entity named "%s" already exists in this location. File could not be copied'
                             % ent.name)
    profile = syn.getUserProfile()
    # get provenance earlier to prevent errors from being called in the end
    # If traceback, set activity to old entity
    if setProvenance == "traceback":
        act = Activity("Copied file", used=ent)
    # if existing, check if provenance exists
    elif setProvenance == "existing":
        try:
            act = syn.getProvenance(ent.id)
        except SynapseHTTPError as e:
            if e.response.status_code == 404:
                act = None
            else:
                raise e
    elif setProvenance is None or setProvenance.lower() == 'none':
        act = None
    else:
        raise ValueError('setProvenance must be one of None, existing, or traceback')
    # Grab entity bundle
    bundle = syn._getEntityBundle(ent.id, version=ent.versionNumber, bitFlags=0x800 | 0x1)
    fileHandle = synapseclient.core.utils.find_data_file_handle(bundle)
    createdBy = fileHandle['createdBy']
    # CHECK: If the user created the file, copy the file by using fileHandleId else copy the fileHandle
    if profile.ownerId == createdBy:
        newdataFileHandleId = ent.dataFileHandleId
    else:
        copiedFileHandle = copyFileHandles(syn, [fileHandle], ["FileEntity"], [bundle['entity']['id']],
                                           [fileHandle['contentType']], [fileHandle['fileName']])
        # Check if failurecodes exist
        copyResult = copiedFileHandle[0]
        if copyResult.get("failureCode") is not None:
            raise ValueError("%s dataFileHandleId: %s" % (copyResult["failureCode"],
                                                          copyResult['originalFileHandleId']))
        newdataFileHandleId = copyResult['newFileHandle']['id']

    new_ent = File(dataFileHandleId=newdataFileHandleId,  name=ent.name, parentId=destinationId)
    # Set annotations here
    if not skipCopyAnnotations:
        new_ent.annotations = ent.annotations
    # Store provenance if act is not None
    if act is not None:
        new_ent = syn.store(new_ent, activity=act)
    else:
        new_ent = syn.store(new_ent)
    # Leave this return statement for test
    return new_ent['id']


def _copyTable(syn, entity, destinationId, updateExisting=False):
    """
    Copies synapse Tables

    :param entity:          A synapse ID of Table Schema

    :param destinationId:   Synapse ID of a project that the Table wants to be copied to

    :param updateExisting:  Can choose to update files that have the same name 
                            Default to False
    """

    print("Getting table %s" % entity)
    myTableSchema = syn.get(entity)
    # CHECK: If Table name already exists, raise value error
    existingEntity = syn.findEntityId(myTableSchema.name, parent=destinationId)
    if existingEntity is not None:
        raise ValueError('An entity named "%s" already exists in this location. Table could not be copied'
                         % myTableSchema.name)

    d = syn.tableQuery('select * from %s' % myTableSchema.id, includeRowIdAndRowVersion=False)

    colIds = myTableSchema.columnIds

    newTableSchema = Schema(name=myTableSchema.name, parent=destinationId, columns=colIds)

    print("Created new table using schema %s" % newTableSchema.name)
    newTable = Table(schema=newTableSchema, values=d.filepath)
    newTable = syn.store(newTable)
    return newTable.schema.id


def _copyLink(syn, entity, destinationId, updateExisting=False):
    """
    Copies Link entities

    :param entity:          A synapse ID of a Link entity

    :param destinationId:   Synapse ID of a folder/project that the file wants to be copied to
    
    :param updateExisting:  Can choose to update files that have the same name 
                            Default to False
    """
    ent = syn.get(entity)
    # CHECK: If Link is in the same parent directory (throw an error)
    if not updateExisting:
        existingEntity = syn.findEntityId(ent.name, parent=destinationId)
        if existingEntity is not None:
            raise ValueError('An entity named "%s" already exists in this location. Link could not be copied'
                             % ent.name)
    newLink = Link(ent.linksTo['targetId'], parent=destinationId,
                   targetVersion=ent.linksTo.get('targetVersionNumber'))
    try:
        newLink = syn.store(newLink)
        return newLink.id
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            print("WARNING: The target of this link %s no longer exists" % ent.id)
            return None
        else:
            raise e


def _getSubWikiHeaders(wikiHeaders, subPageId, mapping=None):
    """
    Function to assist in getting wiki headers of subwikipages
    """
    subPageId = str(subPageId)
    for i in wikiHeaders:
        # This is for the first match 
        # If it isnt the actual parent, it will turn the first match into a parent node which will not have a parentId
        if i['id'] == subPageId:
            if mapping is None:
                i.pop("parentId", None)
                mapping = [i]
            else:
                mapping.append(i)
        elif i.get('parentId') == subPageId:
            mapping = _getSubWikiHeaders(wikiHeaders, subPageId=i['id'], mapping=mapping)
    return mapping


def _updateSynIds(newWikis, wikiIdMap, entityMap):
    print("Updating Synapse references:\n")
    for oldWikiId in wikiIdMap.keys():
        # go through each wiki page once more:
        newWikiId = wikiIdMap[oldWikiId]
        newWiki = newWikis[newWikiId]
        print('Updated Synapse references for Page: %s\n' % newWikiId)
        s = newWiki.markdown

        for oldSynId in entityMap.keys():
            # go through each wiki page once more:
            newSynId = entityMap[oldSynId]
            oldSynId = oldSynId + "\\b"
            s = re.sub(oldSynId, newSynId, s)
        print("Done updating Synapse IDs.\n")
        newWikis[newWikiId].markdown = s
    return newWikis


def _updateInternalLinks(newWikis, wikiIdMap, entity, destinationId):
    print("Updating internal links:\n")
    for oldWikiId in wikiIdMap.keys():
        # go through each wiki page once more:
        newWikiId = wikiIdMap[oldWikiId]
        newWiki = newWikis[newWikiId]
        print("\tUpdating internal links for Page: %s\n" % newWikiId)
        s = newWiki["markdown"]
        # in the markdown field, replace all occurrences of entity/wiki/abc with destinationId/wiki/xyz,
        # where wikiIdMap maps abc->xyz
        # replace <entity>/wiki/<oldWikiId> with <destinationId>/wiki/<newWikiId> 
        for oldWikiId2 in wikiIdMap.keys():
            oldProjectAndWikiId = "%s/wiki/%s\\b" % (entity, oldWikiId2)
            newProjectAndWikiId = "%s/wiki/%s" % (destinationId, wikiIdMap[oldWikiId2])
            s = re.sub(oldProjectAndWikiId, newProjectAndWikiId, s)
        # now replace any last references to entity with destinationId
        s = re.sub(entity, destinationId, s)
        newWikis[newWikiId].markdown = s
    return newWikis


def copyWiki(syn, entity, destinationId, entitySubPageId=None, destinationSubPageId=None, updateLinks=True,
             updateSynIds=True, entityMap=None):
    """
    Copies wikis and updates internal links

    :param syn:                     A synapse object: syn = synapseclient.login()- Must be logged into synapse

    :param entity:                  A synapse ID of an entity whose wiki you want to copy

    :param destinationId:           Synapse ID of a folder/project that the wiki wants to be copied to
    
    :param updateLinks:             Update all the internal links. (e.g. syn1234/wiki/34345 becomes syn3345/wiki/49508)
                                    Defaults to True

    :param updateSynIds:            Update all the synapse ID's referenced in the wikis. (e.g. syn1234 becomes syn2345)
                                    Defaults to True but needs an entityMap

    :param entityMap:               An entity map {'oldSynId','newSynId'} to update the synapse IDs referenced in the
                                    wiki.
                                    Defaults to None 

    :param entitySubPageId:         Can specify subPageId and copy all of its subwikis
                                    Defaults to None, which copies the entire wiki subPageId can be found:
                                    https://www.synapse.org/#!Synapse:syn123/wiki/1234
                                    In this case, 1234 is the subPageId. 

    :param destinationSubPageId:    Can specify destination subPageId to copy wikis to
                                    Defaults to None

    :returns: A list of Objects with three fields: id, title and parentId.
    """

    # Validate input parameters
    if entitySubPageId:
        entitySubPageId = str(int(entitySubPageId))
    if destinationSubPageId:
        destinationSubPageId = str(int(destinationSubPageId))

    oldOwn = syn.get(entity, downloadFile=False)
    # getWikiHeaders fails when there is no wiki

    try:
        oldWikiHeaders = syn.getWikiHeaders(oldOwn)
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            return []
        else:
            raise e

    newOwn = syn.get(destinationId, downloadFile=False)
    wikiIdMap = dict()
    newWikis = dict()
    # If entitySubPageId is given but not destinationSubPageId, set the pageId to "" (will get the root page)
    # A entitySubPage could be copied to a project without any wiki pages, this has to be checked
    newWikiPage = None
    if destinationSubPageId:
        try:
            newWikiPage = syn.getWiki(newOwn, destinationSubPageId)
        except SynapseHTTPError as e:
            if e.response.status_code == 404:
                pass
            else:
                raise e
    if entitySubPageId:
        oldWikiHeaders = _getSubWikiHeaders(oldWikiHeaders, entitySubPageId)

    if not oldWikiHeaders:
        return []

    for wikiHeader in oldWikiHeaders:
        wiki = syn.getWiki(oldOwn, wikiHeader['id'])
        print('Got wiki %s' % wikiHeader['id'])
        if not wiki.get('attachmentFileHandleIds'):
            new_file_handles = []
        else:
            results = [syn._getFileHandleDownload(filehandleId, wiki.id, objectType='WikiAttachment')
                       for filehandleId in wiki['attachmentFileHandleIds']]
            # Get rid of the previews
            nopreviews = [attach['fileHandle'] for attach in results
                          if not attach['fileHandle']['isPreview']]
            contentTypes = [attach['contentType'] for attach in nopreviews]
            fileNames = [attach['fileName'] for attach in nopreviews]
            copiedFileHandles = copyFileHandles(syn, nopreviews, ["WikiAttachment"]*len(nopreviews),
                                                [wiki.id]*len(nopreviews), contentTypes, fileNames)
            # Check if failurecodes exist
            for filehandle in copiedFileHandles:
                if filehandle.get("failureCode") is not None:
                    raise ValueError("%s dataFileHandleId: %s" % (filehandle["failureCode"],
                                                                  filehandle['originalFileHandleId']))
            new_file_handles = [filehandle['newFileHandle']['id'] for filehandle in copiedFileHandles]
        # for some reason some wikis don't have titles?
        if hasattr(wikiHeader, 'parentId'):
            newWikiPage = Wiki(owner=newOwn, title=wiki.get('title', ''), markdown=wiki.markdown,
                               fileHandles=new_file_handles, parentWikiId=wikiIdMap[wiki.parentWikiId])
            newWikiPage = syn.store(newWikiPage)
        else:
            if destinationSubPageId is not None and newWikiPage is not None:
                newWikiPage["attachmentFileHandleIds"] = new_file_handles
                newWikiPage["markdown"] = wiki["markdown"]
                newWikiPage["title"] = wiki.get("title", "")
                # Need to add logic to update titles here
                newWikiPage = syn.store(newWikiPage)
            else:
                newWikiPage = Wiki(owner=newOwn, title=wiki.get("title", ""), markdown=wiki.markdown,
                                   fileHandles=new_file_handles, parentWikiId=destinationSubPageId)
                newWikiPage = syn.store(newWikiPage)
        newWikis[newWikiPage['id']] = newWikiPage
        wikiIdMap[wiki['id']] = newWikiPage['id']

    if updateLinks:
        newWikis = _updateInternalLinks(newWikis, wikiIdMap, entity, destinationId)

    if updateSynIds and entityMap is not None:
        newWikis = _updateSynIds(newWikis, wikiIdMap, entityMap)
    
    print("Storing new Wikis\n")
    for oldWikiId in wikiIdMap.keys():
        newWikiId = wikiIdMap[oldWikiId]
        newWikis[newWikiId] = syn.store(newWikis[newWikiId])
        print("\tStored: %s\n" % newWikiId)
    return syn.getWikiHeaders(newOwn)
