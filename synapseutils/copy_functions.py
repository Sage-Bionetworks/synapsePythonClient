import itertools
import json
import math
import re
import typing

import synapseclient
from synapseclient import (
    Activity,
    Entity,
    File,
    Folder,
    Link,
    Project,
    Schema,
    Table,
    Wiki,
)
from synapseclient.core.cache import Cache
from synapseclient.core.constants.limits import MAX_FILE_HANDLE_PER_COPY_REQUEST
from synapseclient.core.exceptions import SynapseHTTPError

############################################################
#                  Copy Functions                          #
############################################################


def copyFileHandles(
    syn: synapseclient.Synapse,
    fileHandles: typing.List[typing.Union[File, Entity]],
    associateObjectTypes: typing.List[str],
    associateObjectIds: typing.List[str],
    newContentTypes: typing.List[str] = None,
    newFileNames: typing.List[str] = None,
):
    """
    Given a list of fileHandle Ids or Objects, copy the fileHandles

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        fileHandles: List of fileHandle Ids or Objects
        associateObjectTypes: List of associated object types: FileEntity, TableEntity,
                                WikiAttachment, UserProfileAttachment, MessageAttachment,
                                TeamAttachment, SubmissionAttachment, VerificationSubmission
                                (Must be the same length as fileHandles)
        associateObjectIds: List of associated object Ids: If copying a file,
                            the objectId is the synapse id, and if copying a wiki attachment,
                            the object id is the wiki subpage id.
                            (Must be the same length as fileHandles)
        newContentTypes: List of content types. Set each item to a new content type for each file
                            handle, or leave the item as None to keep the original content type.
                            Default None, which keeps all original content types.
        newFileNames: List of filenames. Set each item to a new filename for each file handle,
                        or leave the item as None to keep the original name. Default None,
                        which keeps all original file names.

    Returns:
        List of batch filehandle copy results, can include failureCodes: UNAUTHORIZED and NOT_FOUND

    Raises:
        ValueError: If length of all input arguments are not the same
    """

    # Check if length of all inputs are equal
    if not (
        len(fileHandles) == len(associateObjectTypes) == len(associateObjectIds)
        and (newContentTypes is None or len(newContentTypes) == len(associateObjectIds))
        and (newFileNames is None or len(newFileNames) == len(associateObjectIds))
    ):
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
    for (
        batch_file_handles_ids,
        batch_assoc_obj_types,
        batch_assoc_obj_ids,
        batch_con_type,
        batch_file_name,
    ) in _batch_iterator_generator(
        [
            file_handle_ids,
            associateObjectTypes,
            associateObjectIds,
            newContentTypes,
            newFileNames,
        ],
        MAX_FILE_HANDLE_PER_COPY_REQUEST,
    ):
        batch_copy_results = _copy_file_handles_batch(
            syn,
            batch_file_handles_ids,
            batch_assoc_obj_types,
            batch_assoc_obj_ids,
            batch_con_type,
            batch_file_name,
        )
        master_copy_results_list.extend(batch_copy_results)

    return master_copy_results_list


def _copy_file_handles_batch(
    self: synapseclient.Synapse,
    file_handle_ids: typing.List[str],
    obj_types: typing.List[str],
    obj_ids: typing.List[str],
    new_con_types: typing.List[str],
    new_file_names: typing.List[str],
):
    """
    Given a list of fileHandle Ids, copy the fileHandles.
    This helper makes the POST call and returns the results as a list.

    Arguments:
        self: A Synapse object with user's login, e.g. syn = synapseclient.login()
        file_handle_ids: List of fileHandle Ids or Objects
        obj_types: List of associated object types: FileEntity, TableEntity, WikiAttachment,
                    UserProfileAttachment, MessageAttachment, TeamAttachment, SubmissionAttachment,
                    VerificationSubmission (Must be the same length as fileHandles)
        obj_ids: List of associated object Ids: If copying a file, the objectId is the synapse id,
                                    and if copying a wiki attachment, the object id is the wiki
                                    subpage id. (Must be the same length as fileHandles)
        new_con_types: List of content types (Can change a filetype of a filehandle). Defaults to None.
        new_file_names: List of filenames (Can change a filename of a filehandle). Defaults to None.

    Returns:
        List of batch filehandle copy results, can include failureCodes: UNAUTHORIZED and NOT_FOUND
    """
    copy_file_handle_request = _create_batch_file_handle_copy_request(
        file_handle_ids, obj_types, obj_ids, new_con_types, new_file_names
    )
    # make backend call which performs the copy specified by copy_file_handle_request
    copied_file_handles = self.restPOST(
        "/filehandles/copy",
        body=json.dumps(copy_file_handle_request),
        endpoint=self.fileHandleEndpoint,
    )
    return copied_file_handles.get("copyResults")


def _create_batch_file_handle_copy_request(
    file_handle_ids: typing.List[str],
    obj_types: typing.List[str],
    obj_ids: typing.List[str],
    new_con_types: typing.List[str],
    new_file_names: typing.List[str],
):
    """
    Returns json for file handle copy request

    Arguments:
        file_handle_ids: List of fileHandle Ids
        obj_types: List of associated object types: FileEntity, TableEntity, WikiAttachment,
                                    UserProfileAttachment, MessageAttachment, TeamAttachment,
                                    SubmissionAttachment, VerificationSubmission
                                    (Must be the same length as fileHandles)
        obj_ids: List of associated object Ids: If copying a file, the objectId is the synapse id,
                                    and if copying a wiki attachment, the object id is the wiki
                                    subpage id. (Must be the same length as fileHandles)
        new_con_types: List of content types (Can change a filetype of a filehandle).
        new_file_names: List of filenames (Can change a filename of a filehandle).

    Returns:
        JSON for API call to POST/ filehandles/ copy
    """
    copy_file_handle_request = {"copyRequests": []}
    for (
        file_handle_id,
        obj_type,
        obj_id,
        new_con_type,
        new_file_name,
    ) in itertools.zip_longest(
        file_handle_ids, obj_types, obj_ids, new_con_types, new_file_names
    ):
        # construct JSON object for REST call
        curr_dict = {
            "originalFile": {
                "fileHandleId": file_handle_id,
                "associateObjectId": obj_id,
                "associateObjectType": obj_type,
            },
            "newContentType": new_con_type,
            "newFileName": new_file_name,
        }

        # add copy request to list of requests
        copy_file_handle_request["copyRequests"].append(curr_dict)
    return copy_file_handle_request


def _batch_iterator_generator(iterables: list, batch_size: int):
    """
    Returns a generator over each of the iterable objects in the list iterables

    Arguments:
        iterables: List of iterable objects, all must be same length, len(iterables) >= 1
        batch_size: Integer representing the size of the batch, batch_size >= 1

    Returns:
        A Generator which yields a list of batches for each iterable in iterables

    Raises:
        ValueError: If len(iterables) < 1

    Example:
        _batch_iterator_generator(["ABCDEFG"], 3) --> ["ABC"] ["DEF"] ["G"], on successive calls to next()
        _batch_iterator_generator([[1, 2, 3], [4, 5, 6]], 2) --> [[1, 2], [4, 5]] [[3], [6]]
    """
    if len(iterables) < 1:
        raise ValueError(
            "Must provide at least one iterable in iterables, i.e. len(iterables) >= 1"
        )

    num_batches = math.ceil(len(iterables[0]) / batch_size)
    for i in range(num_batches):
        start = i * batch_size
        end = start + batch_size
        yield [iterables[i][start:end] for i in range(len(iterables))]


def _copy_cached_file_handles(cache: Cache, copiedFileHandles: dict) -> None:
    for copy_result in copiedFileHandles:
        if copy_result.get("failureCode") is None:  # sucessfully copied
            original_cache_path = cache.get(copy_result["originalFileHandleId"])
            if original_cache_path:
                cache.add(copy_result["newFileHandle"]["id"], original_cache_path)


def changeFileMetaData(
    syn: synapseclient.Synapse,
    entity: typing.Union[str, Entity],
    downloadAs: str = None,
    contentType: str = None,
    forceVersion: bool = True,
    name: str = None,
) -> Entity:
    """
    Change File Entity metadata like the download as name.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        entity: Synapse entity Id or object.
        downloadAs: Specify filename to change the filename of a filehandle.
        contentType: Specify content type to change the content type of a filehandle.
        forceVersion: Indicates whether the method should increment the version of
                        the object even if nothing has changed. Defaults to True.
        name: Specify filename to change the filename of the file.

    Returns:
        Synapse Entity

    Example: Using this function
        Updating all file 'downloadAs' names within a folder to match the name of the
        entity.

            import synapseclient
            import synapseutils


            syn = synapseclient.Synapse()
            syn.login()

            MY_FOLDER_TO_UPDATE_ALL_FILES_IN = "syn123"

            for files_to_update in syn.getChildren(
                parent=MY_FOLDER_TO_UPDATE_ALL_FILES_IN, includeTypes=["file"]
            ):
                file_to_check = syn.get(files_to_update["id"], downloadFile=False)
                if file_to_check.name != file_to_check["_file_handle"]["fileName"]:
                    print(
                        f"Updating downloadAs for {file_to_check['_file_handle']['fileName']} to {file_to_check.name}"
                    )

                    synapseutils.changeFileMetaData(
                        syn=syn,
                        entity=file_to_check.id,
                        downloadAs=file_to_check.name,
                        forceVersion=False,
                    )


        Can be used to change the filename, the filename when the file is downloaded,
        or the file content-type without downloading:

            file_entity = syn.get(synid)
            print(os.path.basename(file_entity.path))  ## prints, e.g., "my_file.txt"
            file_entity = synapseutils.changeFileMetaData(syn=syn, entity=file_entity, downloadAs="my_new_downloadAs_name_file.txt", name="my_new_name_file.txt")
            print(os.path.basename(file_entity.path))  ## prints, "my_new_downloadAs_name_file.txt"
            print(file_entity.name) ## prints, "my_new_name_file.txt"
    """
    ent = syn.get(entity, downloadFile=False)
    fileResult = syn._getFileHandleDownload(ent.dataFileHandleId, ent.id)
    ent.contentType = ent.contentType if contentType is None else contentType
    downloadAs = (
        fileResult["fileHandle"]["fileName"] if downloadAs is None else downloadAs
    )
    copiedFileHandle = copyFileHandles(
        syn,
        [ent.dataFileHandleId],
        [ent.concreteType.split(".")[-1]],
        [ent.id],
        [contentType],
        [downloadAs],
    )
    copyResult = copiedFileHandle[0]
    if copyResult.get("failureCode") is not None:
        raise ValueError(
            "%s dataFileHandleId: %s"
            % (copyResult["failureCode"], copyResult["originalFileHandleId"])
        )
    ent.dataFileHandleId = copyResult["newFileHandle"]["id"]
    ent.name = ent.name if name is None else name
    ent = syn.store(ent, forceVersion=forceVersion)
    return ent


def copy(
    syn: synapseclient.Synapse,
    entity: str,
    destinationId: str,
    skipCopyWikiPage: bool = False,
    skipCopyAnnotations: bool = False,
    **kwargs,
) -> typing.Dict[str, str]:
    """
    - This function will assist users in copying entities
        (
        [Tables][synapseclient.table.Table],
        [Links][synapseclient.entity.Link],
        [Files][synapseclient.entity.File],
        [Folders][synapseclient.entity.Folder],
        [Projects][synapseclient.entity.Project]
        ),
      and will recursively copy everything in directories.
    - A Mapping of the old entities to the new entities will be created and all the wikis of each entity
      will also be copied over and links to synapse Ids will be updated.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        entity: A synapse entity ID
        destinationId: Synapse ID of a folder/project that the copied entity is being copied to
        skipCopyWikiPage: Skip copying the wiki pages.
        skipCopyAnnotations: Skips copying the annotations.
        version: (File copy only) Can specify version of a file. Default to None
        updateExisting: (File copy only) When the destination has an entity that has the same name,
                        users can choose to update that entity. It must be the same entity type
                        Default to False
        setProvenance: (File copy only) Has three values to set the provenance of the copied entity:
                        traceback: Sets to the source entity
                        existing: Sets to source entity's original provenance (if it exists)
                        None: No provenance is set
        excludeTypes: (Folder/Project copy only) Accepts a list of entity types (file, table, link)
                        which determines which entity types to not copy. Defaults to an empty list.

    Returns:
        A mapping between the original and copied entity: {'syn1234':'syn33455'}

    Example: Using this function
        Sample copy:

            import synapseutils
            import synapseclient
            syn = synapseclient.login()
            synapseutils.copy(syn, ...)

        Copying Files:

            synapseutils.copy(syn, "syn12345", "syn45678", updateExisting=False, setProvenance = "traceback",version=None)

        Copying Folders/Projects:

            # This will copy everything in the project into the destinationId except files and tables.
            synapseutils.copy(syn, "syn123450","syn345678",excludeTypes=["file","table"])
    """
    updateLinks = kwargs.get("updateLinks", True)
    updateSynIds = kwargs.get("updateSynIds", True)
    entitySubPageId = kwargs.get("entitySubPageId", None)
    destinationSubPageId = kwargs.get("destinationSubPageId", None)

    mapping = _copyRecursive(
        syn, entity, destinationId, skipCopyAnnotations=skipCopyAnnotations, **kwargs
    )
    if not skipCopyWikiPage:
        for oldEnt in mapping:
            copyWiki(
                syn,
                oldEnt,
                mapping[oldEnt],
                entitySubPageId=entitySubPageId,
                destinationSubPageId=destinationSubPageId,
                updateLinks=updateLinks,
                updateSynIds=updateSynIds,
                entityMap=mapping,
            )
    return mapping


def _copyRecursive(
    syn: synapseclient.Synapse,
    entity: str,
    destinationId: str,
    mapping: typing.Dict[str, str] = None,
    skipCopyAnnotations: bool = False,
    **kwargs,
) -> typing.Dict[str, str]:
    """
    Recursively copies synapse entites, but does not copy the wikis

    Arguments:
        syn: A Synapse object with user's login
        entity: A synapse entity ID
        destinationId: Synapse ID of a folder/project that the copied entity is being copied to
        mapping: A mapping of the old entities to the new entities
        skipCopyAnnotations: Skips copying the annotations
                                Default is False

    Returns:
        a mapping between the original and copied entity: {'syn1234':'syn33455'}
    """

    version = kwargs.get("version", None)
    setProvenance = kwargs.get("setProvenance", "traceback")
    excludeTypes = kwargs.get("excludeTypes", [])
    updateExisting = kwargs.get("updateExisting", False)
    if mapping is None:
        mapping = dict()
    # Check that passed in excludeTypes is file, table, and link
    if not isinstance(excludeTypes, list):
        raise ValueError("Excluded types must be a list")
    elif not all([i in ["file", "link", "table"] for i in excludeTypes]):
        raise ValueError(
            "Excluded types can only be a list of these values: file, table, and link"
        )

    ent = syn.get(entity, downloadFile=False)
    if ent.id == destinationId:
        raise ValueError("destinationId cannot be the same as entity id")

    if (isinstance(ent, Project) or isinstance(ent, Folder)) and version is not None:
        raise ValueError("Cannot specify version when copying a project of folder")

    if not isinstance(ent, (Project, Folder, File, Link, Schema, Entity)):
        raise ValueError("Not able to copy this type of file")

    permissions = syn.restGET("/entity/{}/permissions".format(ent.id))
    # Don't copy entities without DOWNLOAD permissions
    if not permissions["canDownload"]:
        syn.logger.warning(
            "%s not copied - this file lacks download permission" % ent.id
        )
        return mapping

    access_requirements = syn.restGET("/entity/{}/accessRequirement".format(ent.id))
    # If there are any access requirements, don't copy files
    if access_requirements["results"]:
        syn.logger.warning(
            "{} not copied - this file has access restrictions".format(ent.id)
        )
        return mapping
    copiedId = None

    if isinstance(ent, Project):
        project = syn.get(destinationId)
        if not isinstance(project, Project):
            raise ValueError(
                "You must give a destinationId of a new project to copy projects"
            )
        copiedId = destinationId
        # Projects include Docker repos, and Docker repos cannot be copied
        # with the Synapse rest API. Entity views currently also aren't
        # supported
        entities = syn.getChildren(
            entity, includeTypes=["folder", "file", "table", "link"]
        )
        for i in entities:
            mapping = _copyRecursive(
                syn,
                i["id"],
                destinationId,
                mapping=mapping,
                skipCopyAnnotations=skipCopyAnnotations,
                **kwargs,
            )

        if not skipCopyAnnotations:
            project.annotations = ent.annotations
            syn.store(project)
    elif isinstance(ent, Folder):
        copiedId = _copyFolder(
            syn,
            ent.id,
            destinationId,
            mapping=mapping,
            skipCopyAnnotations=skipCopyAnnotations,
            **kwargs,
        )
    elif isinstance(ent, File) and "file" not in excludeTypes:
        copiedId = _copyFile(
            syn,
            ent.id,
            destinationId,
            version=version,
            updateExisting=updateExisting,
            setProvenance=setProvenance,
            skipCopyAnnotations=skipCopyAnnotations,
        )
    elif isinstance(ent, Link) and "link" not in excludeTypes:
        copiedId = _copyLink(syn, ent.id, destinationId, updateExisting=updateExisting)
    elif isinstance(ent, Schema) and "table" not in excludeTypes:
        copiedId = _copyTable(syn, ent.id, destinationId, updateExisting=updateExisting)
    # This is currently done because copyLink returns None sometimes
    if copiedId is not None:
        mapping[ent.id] = copiedId
        syn.logger.info("Copied %s to %s" % (ent.id, copiedId))
    else:
        syn.logger.info("%s not copied" % ent.id)
    return mapping


def _copyFolder(
    syn: synapseclient.Synapse,
    entity: str,
    destinationId: str,
    mapping: typing.Dict[str, str] = None,
    skipCopyAnnotations: bool = False,
    **kwargs,
):
    """
    Copies synapse folders

    Arguments:
        entity: A synapse ID of a Folder entity
        destinationId: Synapse ID of a project/folder that the folder wants to be copied to
        mapping: A mapping of synapse IDs to new synapse IDs
        skipCopyAnnotations: Skips copying the annotations
                                Default is False
    """
    oldFolder = syn.get(entity)
    updateExisting = kwargs.get("updateExisting", False)

    if mapping is None:
        mapping = dict()
    # CHECK: If Folder name already exists, raise value error
    if not updateExisting:
        existingEntity = syn.findEntityId(oldFolder.name, parent=destinationId)
        if existingEntity is not None:
            raise ValueError(
                'An entity named "%s" already exists in this location. Folder could not be copied'
                % oldFolder.name
            )

    newFolder = Folder(name=oldFolder.name, parent=destinationId)

    if oldFolder.get("description", None):
        newFolder.description = oldFolder.get("description")

    if not skipCopyAnnotations:
        newFolder.annotations = oldFolder.annotations
    newFolder = syn.store(newFolder)
    entities = syn.getChildren(entity)
    for ent in entities:
        _copyRecursive(
            syn,
            ent["id"],
            newFolder.id,
            mapping,
            skipCopyAnnotations=skipCopyAnnotations,
            **kwargs,
        )
    return newFolder.id


def _copyFile(
    syn: synapseclient.Synapse,
    entity: str,
    destinationId: str,
    version: int = None,
    updateExisting: bool = False,
    setProvenance: str = "traceback",
    skipCopyAnnotations: bool = False,
):
    """
    Copies most recent version of a file to a specified synapse ID.

    Arguments:
        entity: A synapse ID of a File entity
        destinationId: Synapse ID of a folder/project that the file wants to be copied to
        version: Can specify version of a file.
                        Default to None
        updateExisting: Can choose to update files that have the same name
                        Default to False
        setProvenance: Has three values to set the provenance of the copied entity:
                        traceback: Sets to the source entity
                        existing: Sets to source entity's original provenance (if it exists)
                        None: No provenance is set
        skipCopyAnnotations: Skips copying the annotations
                                Default is False
    """
    ent = syn.get(entity, downloadFile=False, version=version, followLink=False)
    # CHECK: If File is in the same parent directory (throw an error) (Can choose to update files)
    if not updateExisting:
        existingEntity = syn.findEntityId(ent.name, parent=destinationId)
        if existingEntity is not None:
            raise ValueError(
                'An entity named "%s" already exists in this location. File could not be copied'
                % ent.name
            )
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
    elif setProvenance is None or setProvenance.lower() == "none":
        act = None
    else:
        raise ValueError("setProvenance must be one of None, existing, or traceback")
    # Grab entity bundle
    bundle = syn._getEntityBundle(
        ent.id,
        version=ent.versionNumber,
        requestedObjects={"includeEntity": True, "includeFileHandles": True},
    )
    fileHandle = synapseclient.core.utils.find_data_file_handle(bundle)
    createdBy = fileHandle["createdBy"]
    # CHECK: If the user created the file, copy the file by using fileHandleId else copy the fileHandle
    if profile.ownerId == createdBy:
        newdataFileHandleId = ent.dataFileHandleId
    else:
        copiedFileHandle = copyFileHandles(
            syn,
            [fileHandle],
            ["FileEntity"],
            [bundle["entity"]["id"]],
            [fileHandle["contentType"]],
            [fileHandle["fileName"]],
        )
        # Check if failurecodes exist
        copyResult = copiedFileHandle[0]
        if copyResult.get("failureCode") is not None:
            raise ValueError(
                "%s dataFileHandleId: %s"
                % (copyResult["failureCode"], copyResult["originalFileHandleId"])
            )
        newdataFileHandleId = copyResult["newFileHandle"]["id"]

    new_ent = File(
        dataFileHandleId=newdataFileHandleId, name=ent.name, parentId=destinationId
    )
    # Set annotations here
    if not skipCopyAnnotations:
        new_ent.annotations = ent.annotations
    # Store provenance if act is not None
    if act is not None:
        new_ent = syn.store(new_ent, activity=act)
    else:
        new_ent = syn.store(new_ent)
    # Leave this return statement for test
    return new_ent["id"]


def _copyTable(syn, entity, destinationId, updateExisting=False):
    """
    Copies synapse Tables

    Arguments:
        entity:          A synapse ID of Table Schema
        destinationId:   Synapse ID of a project that the Table wants to be copied to
        updateExisting:  Can choose to update files that have the same name
                            Default to False
    """

    syn.logger.info("Getting table %s" % entity)
    myTableSchema = syn.get(entity)
    # CHECK: If Table name already exists, raise value error
    existingEntity = syn.findEntityId(myTableSchema.name, parent=destinationId)
    if existingEntity is not None:
        raise ValueError(
            'An entity named "%s" already exists in this location. Table could not be copied'
            % myTableSchema.name
        )

    d = syn.tableQuery(
        "select * from %s" % myTableSchema.id, includeRowIdAndRowVersion=False
    )

    colIds = myTableSchema.columnIds

    newTableSchema = Schema(
        name=myTableSchema.name, parent=destinationId, columns=colIds
    )

    syn.logger.info("Created new table using schema %s" % newTableSchema.name)
    newTable = Table(schema=newTableSchema, values=d.filepath)
    newTable = syn.store(newTable)
    return newTable.schema.id


def _copyLink(syn, entity, destinationId, updateExisting=False):
    """
    Copies Link entities

    Arguments:
        entity:          A synapse ID of a Link entity
        destinationId:   Synapse ID of a folder/project that the file wants to be copied to
        updateExisting:  Can choose to update files that have the same name
                            Default to False
    """
    ent = syn.get(entity)
    # CHECK: If Link is in the same parent directory (throw an error)
    if not updateExisting:
        existingEntity = syn.findEntityId(ent.name, parent=destinationId)
        if existingEntity is not None:
            raise ValueError(
                'An entity named "%s" already exists in this location. Link could not be copied'
                % ent.name
            )
    newLink = Link(
        ent.linksTo["targetId"],
        parent=destinationId,
        targetVersion=ent.linksTo.get("targetVersionNumber"),
    )
    try:
        newLink = syn.store(newLink)
        return newLink.id
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            syn.logger.warning("The target of this link %s no longer exists" % ent.id)
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
        if i["id"] == subPageId:
            if mapping is None:
                i.pop("parentId", None)
                mapping = [i]
            else:
                mapping.append(i)
        elif i.get("parentId") == subPageId:
            mapping = _getSubWikiHeaders(
                wikiHeaders, subPageId=i["id"], mapping=mapping
            )
    return mapping


def _updateSynIds(newWikis, wikiIdMap, entityMap):
    for oldWikiId in wikiIdMap.keys():
        # go through each wiki page once more:
        newWikiId = wikiIdMap[oldWikiId]
        newWiki = newWikis[newWikiId]
        s = newWiki.markdown

        for oldSynId in entityMap.keys():
            # go through each wiki page once more:
            newSynId = entityMap[oldSynId]
            oldSynId = oldSynId + "\\b"
            s = re.sub(oldSynId, newSynId, s)
        newWikis[newWikiId].markdown = s
    return newWikis


def _updateInternalLinks(newWikis, wikiIdMap, entity, destinationId):
    for oldWikiId in wikiIdMap.keys():
        # go through each wiki page once more:
        newWikiId = wikiIdMap[oldWikiId]
        newWiki = newWikis[newWikiId]
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


def copyWiki(
    syn,
    entity,
    destinationId,
    entitySubPageId=None,
    destinationSubPageId=None,
    updateLinks=True,
    updateSynIds=True,
    entityMap=None,
):
    """
    Copies wikis and updates internal links

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        entity: A synapse ID of an entity whose wiki you want to copy
        destinationId: Synapse ID of a folder/project that the wiki wants to be copied to
        updateLinks: Update all the internal links.
                     (e.g. syn1234/wiki/34345 becomes syn3345/wiki/49508)
        updateSynIds: Update all the synapse ID's referenced in the wikis.
                        (e.g. syn1234 becomes syn2345)
                        Defaults to True but needs an entityMap
        entityMap: An entity map {'oldSynId','newSynId'} to update the synapse IDs
                    referenced in the wiki.
        entitySubPageId: Can specify subPageId and copy all of its subwikis
                            Defaults to None, which copies the entire wiki subPageId can be found:
                            https://www.synapse.org/#!Synapse:syn123/wiki/1234
                            In this case, 1234 is the subPageId.
        destinationSubPageId: Can specify destination subPageId to copy wikis to.

    Returns:
        A list of Objects with three fields: id, title and parentId.
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
        wiki = syn.getWiki(oldOwn, wikiHeader["id"])
        syn.logger.info("Got wiki %s" % wikiHeader["id"])
        if not wiki.get("attachmentFileHandleIds"):
            new_file_handles = []
        else:
            results = [
                syn._getFileHandleDownload(
                    filehandleId, wiki.id, objectType="WikiAttachment"
                )
                for filehandleId in wiki["attachmentFileHandleIds"]
            ]
            # Get rid of the previews
            nopreviews = [
                attach["fileHandle"]
                for attach in results
                if not attach["fileHandle"]["isPreview"]
            ]
            contentTypes = [attach["contentType"] for attach in nopreviews]
            fileNames = [attach["fileName"] for attach in nopreviews]
            copiedFileHandles = copyFileHandles(
                syn,
                nopreviews,
                ["WikiAttachment"] * len(nopreviews),
                [wiki.id] * len(nopreviews),
                contentTypes,
                fileNames,
            )
            # Check if failurecodes exist
            for filehandle in copiedFileHandles:
                if filehandle.get("failureCode") is not None:
                    raise ValueError(
                        "%s dataFileHandleId: %s"
                        % (
                            filehandle["failureCode"],
                            filehandle["originalFileHandleId"],
                        )
                    )
            new_file_handles = [
                filehandle["newFileHandle"]["id"] for filehandle in copiedFileHandles
            ]
        # for some reason some wikis don't have titles?
        if hasattr(wikiHeader, "parentId"):
            newWikiPage = Wiki(
                owner=newOwn,
                title=wiki.get("title", ""),
                markdown=wiki.markdown,
                fileHandles=new_file_handles,
                parentWikiId=wikiIdMap[wiki.parentWikiId],
            )
            newWikiPage = syn.store(newWikiPage)
        else:
            if destinationSubPageId is not None and newWikiPage is not None:
                newWikiPage["attachmentFileHandleIds"] = new_file_handles
                newWikiPage["markdown"] = wiki["markdown"]
                newWikiPage["title"] = wiki.get("title", "")
                # Need to add logic to update titles here
                newWikiPage = syn.store(newWikiPage)
            else:
                newWikiPage = Wiki(
                    owner=newOwn,
                    title=wiki.get("title", ""),
                    markdown=wiki.markdown,
                    fileHandles=new_file_handles,
                    parentWikiId=destinationSubPageId,
                )
                newWikiPage = syn.store(newWikiPage)
        newWikis[newWikiPage["id"]] = newWikiPage
        wikiIdMap[wiki["id"]] = newWikiPage["id"]

    if updateLinks:
        syn.logger.info("Updating internal links:\n")
        newWikis = _updateInternalLinks(newWikis, wikiIdMap, entity, destinationId)
        syn.logger.info("Done updating internal links.\n")

    if updateSynIds and entityMap is not None:
        syn.logger.info("Updating Synapse references:\n")
        newWikis = _updateSynIds(newWikis, wikiIdMap, entityMap)
        syn.logger.info("Done updating Synapse IDs.\n")

    syn.logger.info("Storing new Wikis\n")
    for oldWikiId in wikiIdMap.keys():
        newWikiId = wikiIdMap[oldWikiId]
        newWikis[newWikiId] = syn.store(newWikis[newWikiId])
        syn.logger.info("\tStored: %s\n" % newWikiId)
    return syn.getWikiHeaders(newOwn)
