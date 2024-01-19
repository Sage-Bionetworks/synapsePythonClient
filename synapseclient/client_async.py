import os
import collections
import json
import shutil

from synapseclient import Synapse, Activity, Wiki
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseProvenanceError,
)
from .entity import (
    Entity,
    File,
    Folder,
    Versionable,
    split_entity_namespaces,
    is_versionable,
)

from synapseclient.core.utils import (
    id_of,
    get_properties,
    find_data_file_handle,
)

from .annotations import (
    from_synapse_annotations,
    to_synapse_annotations,
    Annotations,
    check_annotations_changed,
)
from synapseclient.core.retry import (
    with_retry_async,
)
from synapseclient.core import utils

from synapseclient.core.upload.upload_functions import (
    upload_file_handle_async,
    upload_synapse_s3,
)

from dataclasses import dataclass
from typing import Dict, Union
from opentelemetry import trace

tracer = trace.get_tracer("synapseclient")


@dataclass
class SynapseAsync(object):
    client: Synapse

    async def get(self, entity, **kwargs):
        """
        Gets a Synapse entity from the repository service.

        Arguments:
            entity:           A Synapse ID, a Synapse Entity object, a plain dictionary in which 'id' maps to a
                                Synapse ID or a local file that is stored in Synapse (found by the file MD5)
            version:          The specific version to get.
                                Defaults to the most recent version.
            downloadFile:     Whether associated files(s) should be downloaded.
                                Defaults to True.
            downloadLocation: Directory where to download the Synapse File Entity.
                                Defaults to the local cache.
            followLink:       Whether the link returns the target Entity.
                                Defaults to False.
            ifcollision:      Determines how to handle file collisions.
                                May be "overwrite.local", "keep.local", or "keep.both".
                                Defaults to "keep.both".
            limitSearch:      A Synanpse ID used to limit the search in Synapse if entity is specified as a local
                                file.  That is, if the file is stored in multiple locations in Synapse only the ones
                                in the specified folder/project will be returned.

        Returns:
            A new Synapse Entity object of the appropriate type.

        Example: Using this function
            Download file into cache

                entity = syn.get('syn1906479')
                print(entity.name)
                print(entity.path)

            Download file into current working directory

                entity = syn.get('syn1906479', downloadLocation='.')
                print(entity.name)
                print(entity.path)

            Determine the provenance of a locally stored file as indicated in Synapse

                entity = syn.get('/path/to/file.txt', limitSearch='syn12312')
                print(syn.getProvenance(entity))
        """
        with tracer.start_as_current_span("SynapseAsync::get_async"):
            # If entity is a local file determine the corresponding synapse entity
            if isinstance(entity, str) and os.path.isfile(entity):
                bundle = await self._get_from_file(
                    entity, kwargs.pop("limitSearch", None)
                )
                kwargs["downloadFile"] = False
                kwargs["path"] = entity

            elif isinstance(entity, str) and not utils.is_synapse_id_str(entity):
                raise SynapseFileNotFoundError(
                    (
                        "The parameter %s is neither a local file path "
                        " or a valid entity id" % entity
                    )
                )
            # have not been saved entities
            elif isinstance(entity, Entity) and not entity.get("id"):
                raise ValueError(
                    "Cannot retrieve entity that has not been saved."
                    " Please use syn.store() to save your entity and try again."
                )
            else:
                version = kwargs.get("version", None)
                bundle = await self._get_entity_bundle(entity, version)
            # Check and warn for unmet access requirements
            self.client._check_entity_restrictions(
                bundle, entity, kwargs.get("downloadFile", True)
            )

            return_data = await self._get_with_entity_bundle(
                entityBundle=bundle, entity=entity, **kwargs
            )
            trace.get_current_span().set_attributes(
                {
                    "synapse.id": return_data.get("id", ""),
                    "synapse.concrete_type": return_data.get("concreteType", ""),
                }
            )
            return return_data

    async def store(
        self,
        obj,
        *,
        createOrUpdate=True,
        forceVersion=True,
        versionLabel=None,
        isRestricted=False,
        activity=None,
        used=None,
        executed=None,
        activityName=None,
        activityDescription=None,
    ):
        """
        Creates a new Entity or updates an existing Entity, uploading any files in the process.

        Arguments:
            obj: A Synapse Entity, Evaluation, or Wiki
            used: The Entity, Synapse ID, or URL used to create the object (can also be a list of these)
            executed: The Entity, Synapse ID, or URL representing code executed to create the object
                        (can also be a list of these)
            activity: Activity object specifying the user's provenance.
            activityName: Activity name to be used in conjunction with *used* and *executed*.
            activityDescription: Activity description to be used in conjunction with *used* and *executed*.
            createOrUpdate: Indicates whether the method should automatically perform an update if the 'obj'
                            conflicts with an existing Synapse object.
            forceVersion: Indicates whether the method should increment the version of the object even if nothing
                            has changed.
            versionLabel: Arbitrary string used to label the version.
            isRestricted: If set to true, an email will be sent to the Synapse access control team to start the
                            process of adding terms-of-use or review board approval for this entity.
                            You will be contacted with regards to the specific data being restricted and the
                            requirements of access.

        Returns:
            A Synapse Entity, Evaluation, or Wiki

        Example: Using this function
            Creating a new Project:

                from synapseclient import Project

                project = Project('My uniquely named project')
                project = syn.store(project)

            Adding files with [provenance (aka: Activity)][synapseclient.Activity]:

                from synapseclient import File, Activity

            A synapse entity *syn1906480* contains data and an entity *syn1917825* contains code

                activity = Activity(
                    'Fancy Processing',
                    description='No seriously, really fancy processing',
                    used=['syn1906480', 'http://data_r_us.com/fancy/data.txt'],
                    executed='syn1917825')

                test_entity = File('/path/to/data/file.xyz', description='Fancy new data', parent=project)
                test_entity = syn.store(test_entity, activity=activity)

        """
        with tracer.start_as_current_span("SynapseAsync::store"):
            # SYNPY-1031: activity must be Activity object or code will fail later
            if activity:
                if not isinstance(activity, Activity):
                    raise ValueError("activity should be synapseclient.Activity object")
            # _before_store hook
            # give objects a chance to do something before being stored
            if hasattr(obj, "_before_synapse_store"):
                # TODO: Include this?
                obj._before_synapse_store(self)

            # _synapse_store hook
            # for objects that know how to store themselves
            if hasattr(obj, "_synapse_store"):
                # TODO: Include this?
                return obj._synapse_store(self)

            # Handle all non-Entity objects
            if not (isinstance(obj, Entity) or type(obj) == dict):
                if isinstance(obj, Wiki):
                    return self._store_wiki(obj, createOrUpdate)

                if "id" in obj:  # If ID is present, update
                    trace.get_current_span().set_attributes({"synapse.id": obj["id"]})
                    return type(obj)(**(await self.rest_put(obj.putURI(), obj.json())))

                try:  # If no ID is present, attempt to POST the object
                    trace.get_current_span().set_attributes({"synapse.id": ""})
                    return type(obj)(
                        **(await self.rest_post(obj.postURI(), obj.json()))
                    )

                except SynapseHTTPError as err:
                    # If already present and we want to update attempt to get the object content
                    if createOrUpdate and err.response.status_code == 409:
                        newObj = await self.rest_get(obj.getByNameURI(obj.name))
                        newObj.update(obj)
                        obj = type(obj)(**newObj)
                        trace.get_current_span().set_attributes(
                            {"synapse.id": obj["id"]}
                        )
                        obj.update(await self.rest_put(obj.putURI(), obj.json()))
                        return obj
                    raise

            # If the input object is an Entity or a dictionary
            entity = obj
            properties, annotations, local_state = split_entity_namespaces(entity)
            bundle = None
            # Explicitly set an empty versionComment property if none is supplied,
            # otherwise an existing entity bundle's versionComment will be copied to the update.
            properties["versionComment"] = (
                properties["versionComment"] if "versionComment" in properties else None
            )

            # Anything with a path is treated as a cache-able item
            # Only Files are expected in the following logic
            if entity.get("path", False) and not isinstance(obj, Folder):
                if "concreteType" not in properties:
                    properties["concreteType"] = File._synapse_entity_type
                # Make sure the path is fully resolved
                entity["path"] = os.path.expanduser(entity["path"])

                # Check if the File already exists in Synapse by fetching metadata on it
                bundle = await self._get_entity_bundle(entity)

                if bundle:
                    if createOrUpdate:
                        # update our properties from the existing bundle so that we have
                        # enough to process this as an entity update.
                        properties = {**bundle["entity"], **properties}

                    # Check if the file should be uploaded
                    fileHandle = find_data_file_handle(bundle)
                    if (
                        fileHandle
                        and fileHandle["concreteType"]
                        == "org.sagebionetworks.repo.model.file.ExternalFileHandle"
                    ):
                        # switching away from ExternalFileHandle or the url was updated
                        needs_upload = entity["synapseStore"] or (
                            fileHandle["externalURL"] != entity["externalURL"]
                        )
                    else:
                        # Check if we need to upload a new version of an existing
                        # file. If the file referred to by entity['path'] has been
                        # modified, we want to upload the new version.
                        # If synapeStore is false then we must upload a ExternalFileHandle
                        needs_upload = not entity[
                            "synapseStore"
                        ] or not self.client.cache.contains(
                            bundle["entity"]["dataFileHandleId"], entity["path"]
                        )
                elif entity.get("dataFileHandleId", None) is not None:
                    needs_upload = False
                else:
                    needs_upload = True

                if needs_upload:
                    local_state_fh = local_state.get("_file_handle", {})
                    synapseStore = local_state.get("synapseStore", True)
                    fileHandle = await upload_file_handle_async(
                        self,
                        entity["parentId"],
                        local_state["path"]
                        if (synapseStore or local_state_fh.get("externalURL") is None)
                        else local_state_fh.get("externalURL"),
                        synapseStore=synapseStore,
                        md5=local_state_fh.get("contentMd5"),
                        file_size=local_state_fh.get("contentSize"),
                        mimetype=local_state_fh.get("contentType"),
                    )
                    properties["dataFileHandleId"] = fileHandle["id"]
                    local_state["_file_handle"] = fileHandle

                elif "dataFileHandleId" not in properties:
                    # Handle the case where the Entity lacks an ID
                    # But becomes an update() due to conflict
                    properties["dataFileHandleId"] = bundle["entity"][
                        "dataFileHandleId"
                    ]

                # update the file_handle metadata if the FileEntity's FileHandle id has changed
                local_state_fh_id = local_state.get("_file_handle", {}).get("id")
                if (
                    local_state_fh_id
                    and properties["dataFileHandleId"] != local_state_fh_id
                ):
                    local_state["_file_handle"] = find_data_file_handle(
                        await self._get_entity_bundle(
                            properties["id"],
                            requestedObjects={
                                "includeEntity": True,
                                "includeFileHandles": True,
                            },
                        )
                    )

                    # check if we already have the filehandleid cached somewhere
                    cached_path = self.client.cache.get(properties["dataFileHandleId"])
                    if cached_path is None:
                        local_state["path"] = None
                        local_state["cacheDir"] = None
                        local_state["files"] = []
                    else:
                        local_state["path"] = cached_path
                        local_state["cacheDir"] = os.path.dirname(cached_path)
                        local_state["files"] = [os.path.basename(cached_path)]

            # Create or update Entity in Synapse
            if "id" in properties:
                trace.get_current_span().set_attributes(
                    {"synapse.id": properties["id"]}
                )
                properties = await self._update_entity(
                    properties, forceVersion, versionLabel
                )
            else:
                # If Link, get the target name, version number and concrete type and store in link properties
                if properties["concreteType"] == "org.sagebionetworks.repo.model.Link":
                    target_properties = await self._get_entity(
                        properties["linksTo"]["targetId"],
                        version=properties["linksTo"].get("targetVersionNumber"),
                    )
                    if target_properties["parentId"] == properties["parentId"]:
                        raise ValueError(
                            "Cannot create a Link to an entity under the same parent."
                        )
                    properties["linksToClassName"] = target_properties["concreteType"]
                    if (
                        target_properties.get("versionNumber") is not None
                        and properties["linksTo"].get("targetVersionNumber") is not None
                    ):
                        properties["linksTo"][
                            "targetVersionNumber"
                        ] = target_properties["versionNumber"]
                    properties["name"] = target_properties["name"]
                try:
                    entity_bundle = await self._create_entity_bundle(
                        entity=properties, annotations=annotations
                    )
                    properties = entity_bundle["entity"]
                    is_create = True
                except SynapseHTTPError as ex:
                    if createOrUpdate and ex.response.status_code == 409:
                        # Get the existing Entity's ID via the name and parent
                        existing_entity_id = await self.find_entity_id(
                            properties["name"], properties.get("parentId", None)
                        )
                        if existing_entity_id is None:
                            raise

                        # get existing properties and annotations
                        if not bundle:
                            bundle = await self._get_entity_bundle(
                                existing_entity_id,
                                requestedObjects={
                                    "includeEntity": True,
                                    "includeAnnotations": True,
                                },
                            )

                        properties = {**bundle["entity"], **properties}

                        # we additionally merge the annotations under the assumption that a missing annotation
                        # from a resolved conflict represents an newer annotation that should be preserved
                        # rather than an intentionally deleted annotation.
                        annotations = {
                            **from_synapse_annotations(bundle["annotations"]),
                            **annotations,
                        }

                        properties = await self._update_entity(
                            properties, forceVersion, versionLabel
                        )

                    else:
                        raise

            # Deal with access restrictions
            if isRestricted:
                await self._create_access_requirement_if_none(properties)

            # Update annotations
            if (not bundle and annotations and not is_create) or (
                bundle and check_annotations_changed(bundle["annotations"], annotations)
            ):
                annotations = await self.set_annotations(
                    Annotations(properties["id"], properties["etag"], annotations)
                )
                properties["etag"] = annotations.etag

            # If the parameters 'used' or 'executed' are given, create an Activity object
            if used or executed:
                if activity is not None:
                    raise SynapseProvenanceError(
                        "Provenance can be specified as an Activity object or as used/executed"
                        " item(s), but not both."
                    )
                activity = Activity(
                    name=activityName,
                    description=activityDescription,
                    used=used,
                    executed=executed,
                )

            # If we have an Activity, set it as the Entity's provenance record
            if activity:
                await self.set_provenance(properties, activity)

                # 'etag' has changed, so get the new Entity
                properties = await self._get_entity(properties)

            # Return the updated Entity object
            entity = Entity.create(properties, annotations, local_state)
            return_data = await self.get(entity, downloadFile=False)

            trace.get_current_span().set_attributes(
                {
                    "synapse.id": return_data.get("id", ""),
                    "synapse.concrete_type": entity.get("concreteType", ""),
                }
            )
            return return_data

    async def get_wiki(self, owner, subpageId=None, version=None):
        """
        Get a [synapseclient.wiki.Wiki][] object from Synapse. Uses wiki2 API which supports versioning.

        Arguments:
            owner: The entity to which the Wiki is attached
            subpageId: The id of the specific sub-page or None to get the root Wiki page
            version: The version of the page to retrieve or None to retrieve the latest

        Returns:
            A [synapseclient.wiki.Wiki][] object
        """
        with tracer.start_as_current_span("SynapseAsync::get_wiki"):
            uri = "/entity/{ownerId}/wiki2".format(ownerId=id_of(owner))
            if subpageId is not None:
                uri += "/{wikiId}".format(wikiId=subpageId)
            if version is not None:
                uri += "?wikiVersion={version}".format(version=version)

            wiki = await self.rest_get(uri)
            wiki["owner"] = owner
            wiki = Wiki(**wiki)

            path = self.client.cache.get(wiki.markdownFileHandleId)
            if not path:
                cache_dir = self.client.cache.get_cache_dir(wiki.markdownFileHandleId)
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                path = self.client._downloadFileHandle(
                    wiki["markdownFileHandleId"],
                    wiki["id"],
                    "WikiMarkdown",
                    os.path.join(cache_dir, str(wiki.markdownFileHandleId) + ".md"),
                )
            try:
                import gzip

                with gzip.open(path) as f:
                    markdown = f.read().decode("utf-8")
            except IOError:
                with open(path) as f:
                    markdown = f.read().decode("utf-8")

            wiki.markdown = markdown
            wiki.markdown_path = path

            return wiki

    async def _store_wiki(self, wiki: Wiki, createOrUpdate: bool) -> Wiki:
        """
        Stores or updates the given Wiki.

        Arguments:
            wiki:           A Wiki object
            createOrUpdate: Indicates whether the method should automatically perform an update if the 'obj'
                            conflicts with an existing Synapse object.

        Returns:
            An updated Wiki object
        """
        with tracer.start_as_current_span("SynapseAsync::_store_wiki"):
            # Make sure the file handle field is a list
            if "attachmentFileHandleIds" not in wiki:
                wiki["attachmentFileHandleIds"] = []

            # Convert all attachments into file handles
            if wiki.get("attachments") is not None:
                for attachment in wiki["attachments"]:
                    fileHandle = upload_synapse_s3(self, attachment)
                    wiki["attachmentFileHandleIds"].append(fileHandle["id"])
                del wiki["attachments"]

            # Perform an update if the Wiki has an ID
            if "id" in wiki:
                updated_wiki = Wiki(
                    owner=wiki.ownerId,
                    **(await self.rest_put(wiki.putURI(), wiki.json())),
                )

            # Perform a create if the Wiki has no ID
            else:
                try:
                    updated_wiki = Wiki(
                        owner=wiki.ownerId,
                        **(await self.rest_post(wiki.postURI(), wiki.json())),
                    )
                except SynapseHTTPError as err:
                    # If already present we get an unhelpful SQL error
                    if createOrUpdate and (
                        (
                            err.response.status_code == 400
                            and "DuplicateKeyException" in err.message
                        )
                        or err.response.status_code == 409
                    ):
                        existing_wiki = await self.get_wiki(wiki.ownerId)

                        # overwrite everything except for the etag (this will keep unmodified fields in the existing wiki)
                        etag = existing_wiki["etag"]
                        existing_wiki.update(wiki)
                        existing_wiki.etag = etag

                        updated_wiki = Wiki(
                            owner=wiki.ownerId,
                            **(
                                await self.rest_put(
                                    existing_wiki.putURI(), existing_wiki.json()
                                )
                            ),
                        )
                    else:
                        raise
            return updated_wiki

    async def _get_entity_bundle(
        self,
        entity: Union[Entity, str],
        version: int = None,
        requestedObjects: Dict[str, bool] = None,
    ) -> Dict[str, Union[dict, str, int, bool]]:
        """
        Gets some information about the Entity.

        Arguments:
            entity:           A Synapse Entity or Synapse ID
            version:          The entity's version. Defaults to None meaning most recent version.
            requestedObjects: A dictionary indicating settings for what to include.

        Default value for requestedObjects is:

            requestedObjects = {'includeEntity': True,
                                'includeAnnotations': True,
                                'includeFileHandles': True,
                                'includeRestrictionInformation': True}

        Keys available for requestedObjects:

            includeEntity
            includeAnnotations
            includePermissions
            includeEntityPath
            includeHasChildren
            includeAccessControlList
            includeFileHandles
            includeTableBundle
            includeRootWikiId
            includeBenefactorACL
            includeDOIAssociation
            includeFileName
            includeThreadCount
            includeRestrictionInformation


        Keys with values set to False may simply be omitted.
        For example, we might ask for an entity bundle containing file handles, annotations, and properties:
            requested_objects = {'includeEntity':True
                                 'includeAnnotations':True,
                                 'includeFileHandles':True}
            bundle = syn._getEntityBundle('syn111111', )

        Returns:
            An EntityBundle with the requested fields or by default Entity header, annotations, unmet access
            requirements, and file handles
        """
        # If 'entity' is given without an ID, try to find it by 'parentId' and 'name'.
        # Use case:
        #     If the user forgets to catch the return value of a syn.store(e)
        #     this allows them to recover by doing: e = syn.get(e)
        if requestedObjects is None:
            requestedObjects = {
                "includeEntity": True,
                "includeAnnotations": True,
                "includeFileHandles": True,
                "includeRestrictionInformation": True,
            }
        if (
            isinstance(entity, collections.abc.Mapping)
            and "id" not in entity
            and "name" in entity
        ):
            entity = await self.find_entity_id(
                entity["name"], entity.get("parentId", None)
            )

        # Avoid an exception from finding an ID from a NoneType
        try:
            id_of(entity)
        except ValueError:
            return None

        if version is not None:
            uri = f"/entity/{id_of(entity)}/version/{int(version):d}/bundle2"
        else:
            uri = f"/entity/{id_of(entity)}/bundle2"
        bundle = await self.rest_post(uri, body=json.dumps(requestedObjects))

        return bundle

    async def delete(
        self,
        obj,
        version=None,
    ):
        """
        Removes an object from Synapse.

        Arguments:
            obj: An existing object stored on Synapse such as Evaluation, File, Project, or Wiki
            version: For entities, specify a particular version to delete.
        """
        with tracer.start_as_current_span("SynapseAsync::delete"):
            # Handle all strings as the Entity ID for backward compatibility
            if isinstance(obj, str):
                entity_id = id_of(obj)
                trace.get_current_span().set_attributes({"synapse.id": entity_id})
                if version:
                    await self.rest_delete(uri=f"/entity/{entity_id}/version/{version}")
                else:
                    await self.rest_delete(uri=f"/entity/{entity_id}")
            elif hasattr(obj, "_synapse_delete"):
                return obj._synapse_delete(self)
            else:
                try:
                    if isinstance(obj, Versionable):
                        await self.rest_delete(obj.deleteURI(versionNumber=version))
                    else:
                        await self.rest_delete(obj.deleteURI())
                except AttributeError:
                    raise SynapseError(
                        f"Can't delete a {type(obj)}. Please specify a Synapse object or id"
                    )

    async def upload_file_handle(
        self, path, parent, synapseStore=True, mimetype=None, md5=None, file_size=None
    ):
        """Uploads the file in the provided path (if necessary) to a storage location based on project settings.
        Returns a new FileHandle as a dict to represent the stored file.

        Arguments:
            parent: Parent of the entity to which we upload.
            path:   File path to the file being uploaded
            synapseStore: If False, will not upload the file, but instead create an ExternalFileHandle that references
                            the file on the local machine.
                            If True, will upload the file based on StorageLocation determined by the entity_parent_id
            mimetype: The MIME type metadata for the uploaded file
            md5: The MD5 checksum for the file, if known. Otherwise if the file is a local file, it will be calculated
                    automatically.
            file_size: The size the file, if known. Otherwise if the file is a local file, it will be calculated
                        automatically.
            file_type: The MIME type the file, if known. Otherwise if the file is a local file, it will be calculated
                        automatically.

        Returns:
            A dict of a new FileHandle as a dict that represents the uploaded file
        """
        return await upload_file_handle_async(
            self, parent, path, synapseStore, md5, file_size, mimetype
        )

    async def set_annotations(self, annotations: Annotations):
        """
        Store annotations for an Entity in the Synapse Repository.

        Arguments:
            annotations: A [synapseclient.annotations.Annotations][] of annotation names and values,
                            with the id and etag attribute set

        Returns:
            The updated [synapseclient.annotations.Annotations][] for the entity

        Example: Using this function
            Getting annotations, adding a new annotation, and updating the annotations:

                annos = syn.get_annotations('syn123')

            `annos` will contain the id and etag associated with the entity upon retrieval

                print(annos.id)
                > syn123
                print(annos.etag)
                > 7bdb83e9-a50a-46e4-987a-4962559f090f   (Usually some UUID in the form of a string)

            Returned `annos` object from `get_annotations()` can be used as if it were a dict.
            Set key 'foo' to have value of 'bar' and 'baz'

                annos['foo'] = ['bar', 'baz']

            Single values will automatically be wrapped in a list once stored

                annos['qwerty'] = 'asdf'

            Store the annotations

                annos = syn.set_annotations(annos)

                print(annos)
                > {'foo':['bar','baz], 'qwerty':['asdf']}
        """
        with tracer.start_as_current_span("SynapseAsync::set_annotations_async"):
            if not isinstance(annotations, Annotations):
                raise TypeError("Expected a synapseclient.Annotations object")

            synapseAnnos = to_synapse_annotations(annotations)

            entity_id = id_of(annotations)
            trace.get_current_span().set_attributes({"synapse.id": entity_id})

            return from_synapse_annotations(
                await self.rest_put(
                    f"/entity/{entity_id}/annotations2",
                    body=json.dumps(synapseAnnos),
                )
            )

    async def set_provenance(self, entity, activity) -> Activity:
        """
        Stores a record of the code and data used to derive a Synapse entity.

        Arguments:
            entity:   An Entity or Synapse ID to modify
            activity: A [synapseclient.activity.Activity][]

        Returns:
            An updated [synapseclient.activity.Activity][] object
        """
        with tracer.start_as_current_span("SynapseAsync::set_provenance_async"):
            # Assert that the entity was generated by a given Activity.
            activity = await self._save_activity(activity)

            entity_id = id_of(entity)
            # assert that an entity is generated by an activity
            uri = "/entity/%s/generatedBy?generatedBy=%s" % (entity_id, activity["id"])
            activity = Activity(data=await self.rest_put(uri))

            trace.get_current_span().set_attributes({"synapse.id": entity_id})
            return activity

    async def _save_activity(self, activity: Activity) -> Activity:
        """
        Save the Activity

        Arguments:
            activity: The Activity to be saved

        Returns:
            An Activity object
        """
        if "id" in activity:
            # We're updating provenance
            uri = "/activity/%s" % activity["id"]
            activity = Activity(data=await self.rest_put(uri, json.dumps(activity)))
        else:
            activity = await self.rest_post("/activity", body=json.dumps(activity))
        return activity

    async def _get_from_file(
        self, filepath: str, limitSearch: str = None
    ) -> Dict[str, dict]:
        """
        Gets a Synapse entityBundle based on the md5 of a local file.
        See [get][synapseclient.Synapse.get].

        Arguments:
            filepath:    The path to local file
            limitSearch: Limits the places in Synapse where the file is searched for.

        Raises:
            SynapseFileNotFoundError: If the file is not in Synapse.

        Returns:
            A Synapse entityBundle
        """
        with tracer.start_as_current_span("SynapseAsync::_get_from_file"):
            results = await self.rest_get(
                "/entity/md5/%s" % utils.md5_for_file(filepath).hexdigest()
            )["results"]
            if limitSearch is not None:
                # Go through and find the path of every entity found
                paths = [
                    await self.rest_get("/entity/%s/path" % ent["id"])
                    for ent in results
                ]
                # Filter out all entities whose path does not contain limitSearch
                results = [
                    ent
                    for ent, path in zip(results, paths)
                    if utils.is_in_path(limitSearch, path)
                ]
            if len(results) == 0:  # None found
                raise SynapseFileNotFoundError(
                    "File %s not found in Synapse" % (filepath,)
                )
            elif len(results) > 1:
                id_txts = "\n".join(
                    ["%s.%i" % (r["id"], r["versionNumber"]) for r in results]
                )
                self.client.logger.warning(
                    "\nThe file %s is associated with many files in Synapse:\n%s\n"
                    "You can limit to files in specific project or folder by setting the limitSearch to the"
                    " synapse Id of the project or folder.\n"
                    "Will use the first one returned: \n"
                    "%s version %i\n"
                    % (filepath, id_txts, results[0]["id"], results[0]["versionNumber"])
                )
            entity = results[0]

            bundle = await self._get_entity_bundle(
                entity, version=entity["versionNumber"]
            )
            self.client.cache.add(bundle["entity"]["dataFileHandleId"], filepath)

            return bundle

    async def _get_default_upload_destination(self, parent_entity):
        with tracer.start_as_current_span(
            "SynapseAsync::_get_default_upload_destination"
        ):
            return await self.rest_get(
                "/entity/%s/uploadDestination" % id_of(parent_entity),
                endpoint=self.client.fileHandleEndpoint,
            )

    async def _get_with_entity_bundle(
        self, entityBundle: dict, entity: Entity = None, **kwargs
    ) -> Entity:
        """
        Creates a [Entity][synapseclient.Entity] from an entity bundle returned by Synapse.
        An existing Entity can be supplied in case we want to refresh a stale Entity.

        Arguments:
            entityBundle: Uses the given dictionary as the meta information of the Entity to get
            entity:       Optional, entity whose local state will be copied into the returned entity
            submission:   Optional, access associated files through a submission rather than through an entity.

        Returns:
            A new Synapse Entity

        Also see:
        - See [get][synapseclient.Synapse.get].
        - See [_getEntityBundle][synapseclient.Synapse._getEntityBundle].
        - See [Entity][synapseclient.Entity].
        """
        with tracer.start_as_current_span("SynapseAsync::_get_with_entity_bundle"):
            # Note: This version overrides the version of 'entity' (if the object is Mappable)
            kwargs.pop("version", None)
            downloadFile = kwargs.pop("downloadFile", True)
            downloadLocation = kwargs.pop("downloadLocation", None)
            ifcollision = kwargs.pop("ifcollision", None)
            submission = kwargs.pop("submission", None)
            followLink = kwargs.pop("followLink", False)
            path = kwargs.pop("path", None)

            # make sure user didn't accidentlaly pass a kwarg that we don't handle
            if kwargs:  # if there are remaining items in the kwargs
                raise TypeError("Unexpected **kwargs: %r" % kwargs)

            # If Link, get target ID entity bundle
            if (
                entityBundle["entity"]["concreteType"]
                == "org.sagebionetworks.repo.model.Link"
                and followLink
            ):
                targetId = entityBundle["entity"]["linksTo"]["targetId"]
                targetVersion = entityBundle["entity"]["linksTo"].get(
                    "targetVersionNumber"
                )
                entityBundle = self._get_entity_bundle(targetId, targetVersion)

            # TODO is it an error to specify both downloadFile=False and downloadLocation?
            # TODO this matters if we want to return already cached files when downloadFile=False

            # Make a fresh copy of the Entity
            local_state = (
                entity.local_state() if entity and isinstance(entity, Entity) else {}
            )
            if path is not None:
                local_state["path"] = path
            properties = entityBundle["entity"]
            annotations = from_synapse_annotations(entityBundle["annotations"])
            entity = Entity.create(properties, annotations, local_state)

            # Handle download of fileEntities
            if isinstance(entity, File):
                # update the entity with FileHandle metadata
                file_handle = next(
                    (
                        handle
                        for handle in entityBundle["fileHandles"]
                        if handle["id"] == entity.dataFileHandleId
                    ),
                    None,
                )
                entity._update_file_handle(file_handle)

                if downloadFile:
                    if file_handle:
                        self._download_file_entity(
                            downloadLocation,
                            entity,
                            ifcollision,
                            submission,
                        )
                    else:  # no filehandle means that we do not have DOWNLOAD permission
                        warning_message = (
                            "WARNING: You have READ permission on this file entity but not DOWNLOAD "
                            "permission. The file has NOT been downloaded."
                        )
                        self.client.logger.warning(
                            "\n"
                            + "!" * len(warning_message)
                            + "\n"
                            + warning_message
                            + "\n"
                            + "!" * len(warning_message)
                            + "\n"
                        )
            return entity

    async def _download_file_entity(
        self,
        downloadLocation: str,
        entity: Entity,
        ifcollision: str,
        submission: str,
    ) -> None:
        """
        Download file entity

        Arguments:
            downloadLocation: The download location
            entity:           The Synapse Entity object
            ifcollision:      Determines how to handle file collisions.
                              May be

                - `overwrite.local`
                - `keep.local`
                - `keep.both`

            submission:       Access associated files through a submission rather than through an entity.
        """
        with tracer.start_as_current_span("SynapseAsync::_download_file_entity"):
            # set the initial local state
            entity.path = None
            entity.files = []
            entity.cacheDir = None

            # check to see if an UNMODIFIED version of the file (since it was last downloaded) already exists
            # this location could be either in .synapseCache or a user specified location to which the user previously
            # downloaded the file
            cached_file_path = self.client.cache.get(
                entity.dataFileHandleId, downloadLocation
            )

            # location in .synapseCache where the file would be corresponding to its FileHandleId
            synapseCache_location = self.client.cache.get_cache_dir(
                entity.dataFileHandleId
            )

            file_name = (
                entity._file_handle.fileName
                if cached_file_path is None
                else os.path.basename(cached_file_path)
            )

            # Decide the best download location for the file
            if downloadLocation is not None:
                # Make sure the specified download location is a fully resolved directory
                downloadLocation = self.client._ensure_download_location_is_directory(
                    downloadLocation
                )
            elif cached_file_path is not None:
                # file already cached so use that as the download location
                downloadLocation = os.path.dirname(cached_file_path)
            else:
                # file not cached and no user-specified location so default to .synapseCache
                downloadLocation = synapseCache_location

            # resolve file path collisions by either overwriting, renaming, or not downloading, depending on the
            # ifcollision value
            downloadPath = self.client._resolve_download_path_collisions(
                downloadLocation,
                file_name,
                ifcollision,
                synapseCache_location,
                cached_file_path,
            )
            if downloadPath is None:
                return

            if cached_file_path is not None:  # copy from cache
                if downloadPath != cached_file_path:
                    # create the foider if it does not exist already
                    if not os.path.exists(downloadLocation):
                        os.makedirs(downloadLocation)
                    shutil.copy(cached_file_path, downloadPath)

            else:  # download the file from URL (could be a local file)
                objectType = (
                    "FileEntity" if submission is None else "SubmissionAttachment"
                )
                objectId = entity["id"] if submission is None else submission

                # reassign downloadPath because if url points to local file (e.g. file://~/someLocalFile.txt)
                # it won't be "downloaded" and, instead, downloadPath will just point to '~/someLocalFile.txt'
                # _downloadFileHandle may also return None to indicate that the download failed
                # TODO: This would need to be converted to an ASYNC function
                downloadPath = self.client._downloadFileHandle(
                    entity.dataFileHandleId, objectId, objectType, downloadPath
                )

                if downloadPath is None or not os.path.exists(downloadPath):
                    return

            # converts the path format from forward slashes back to backward slashes on Windows
            entity.path = os.path.normpath(downloadPath)
            entity.files = [os.path.basename(downloadPath)]
            entity.cacheDir = os.path.dirname(downloadPath)

    async def _get_entity(
        self, entity: Union[str, dict, Entity], version: int = None
    ) -> Dict[str, Union[str, bool]]:
        """
        Get an entity from Synapse.

        Arguments:
            entity:  A Synapse ID, a dictionary representing an Entity, or a Synapse Entity object
            version: The version number to fetch

        Returns:
            A dictionary containing an Entity's properties
        """
        with tracer.start_as_current_span("SynapseAsync::_get_entity"):
            uri = "/entity/" + id_of(entity)
            if version:
                uri += "/version/%d" % version
            return await self.rest_get(uri)

    async def _create_entity(
        self, entity: Union[dict, Entity]
    ) -> Dict[str, Union[str, bool]]:
        """
        Create a new entity in Synapse.

        Arguments:
            entity: A dictionary representing an Entity or a Synapse Entity object

        Returns:
            A dictionary containing an Entity's properties
        """
        with tracer.start_as_current_span("SynapseAsync::_create_entity"):
            return await self.rest_post(
                uri="/entity", body=json.dumps(get_properties(entity))
            )

    async def _create_entity_bundle(
        self, entity: Union[dict, Entity], annotations
    ) -> Dict[str, Union[str, bool]]:
        """
        Create a new entity in Synapse.

        Arguments:
            entity: A dictionary representing an Entity or a Synapse Entity object

        Returns:
            A dictionary containing an Entity's properties
        """
        with tracer.start_as_current_span("SynapseAsync::_create_entity"):
            body = {
                "entity": get_properties(entity),
            }
            if annotations:
                body["annotations"] = to_synapse_annotations(
                    Annotations(id=None, etag=None, values=annotations)
                )
            return await self.rest_post(
                uri="/entity/bundle2/create",
                body=json.dumps(body),
            )

    async def _update_entity(
        self,
        entity: Union[dict, Entity],
        incrementVersion: bool = True,
        versionLabel: str = None,
    ) -> Dict[str, Union[str, bool]]:
        """
        Update an existing entity in Synapse.

        Arguments:
            entity:           A dictionary representing an Entity or a Synapse Entity object
            incrementVersion: Whether to increment the entity version (if Versionable)
            versionLabel:     A label for the entity version (if Versionable)

        Returns:
            A dictionary containing an Entity's properties
        """
        with tracer.start_as_current_span("SynapseAsync::_update_entity"):
            uri = "/entity/%s" % id_of(entity)

            params = {}
            if is_versionable(entity):
                if versionLabel:
                    # a versionLabel implicitly implies incrementing
                    incrementVersion = True
                elif incrementVersion and "versionNumber" in entity:
                    versionLabel = str(entity["versionNumber"] + 1)

                if incrementVersion:
                    entity["versionLabel"] = versionLabel
                    params["newVersion"] = "true"

            return await self.rest_put(
                uri, body=json.dumps(get_properties(entity)), params=params
            )

    async def find_entity_id(self, name, parent=None):
        """
        Find an Entity given its name and parent.

        Arguments:
            name: Name of the entity to find
            parent: An Entity object or the Id of an entity as a string. Omit if searching for a Project by name

        Returns:
            The Entity ID or None if not found
        """
        with tracer.start_as_current_span("SynapseAsync::find_entity_id"):
            # when we want to search for a project by name. set parentId as None instead of ROOT_ENTITY
            entity_lookup_request = {
                "parentId": id_of(parent) if parent else None,
                "entityName": name,
            }
            try:
                return (
                    await self.rest_post(
                        "/entity/child", body=json.dumps(entity_lookup_request)
                    )
                ).get("id")
            except SynapseHTTPError as e:
                if (
                    e.response.status_code == 404
                ):  # a 404 error is raised if the entity does not exist
                    return None
                raise

    async def _create_access_requirement_if_none(
        self, entity: Union[Entity, str]
    ) -> None:
        """
        Checks to see if the given entity has access requirements. If not, then one is added

        Arguments:
            entity: A Synapse ID or a Synapse Entity object
        """
        with tracer.start_as_current_span(
            "SynapseAsync::_create_access_requirement_if_none"
        ):
            existingRestrictions = await self.rest_get(
                "/entity/%s/accessRequirement?offset=0&limit=1" % id_of(entity)
            )
            if len(existingRestrictions["results"]) <= 0:
                await self.rest_post(
                    "/entity/%s/lockAccessRequirement" % id_of(entity), body=""
                )

    async def _get_file_handle_as_creator(
        self, fileHandle: Dict[str, Union[str, int]]
    ) -> Dict[str, Union[str, int]]:
        """
        Retrieve a fileHandle from the fileHandle service.
        Note: You must be the creator of the filehandle to use this method. Otherwise, an 403-Forbidden error will be raised.

        Arguments:
            fileHandle: A fileHandle

        Returns:
            A fileHandle retrieved from the fileHandle service.
        """
        with tracer.start_as_current_span("SynapseAsync::_get_file_handle_as_creator"):
            uri = "/fileHandle/%s" % (id_of(fileHandle),)
            return await self.rest_get(uri, endpoint=self.client.fileHandleEndpoint)

    async def _rest_call(
        self,
        method,
        uri,
        data,
        endpoint,
        headers,
        retryPolicy,
        requests_session_async_synapse,
        **kwargs,
    ):
        """
        Sends an HTTP request to the Synapse server.

        Arguments:
            method:           The method to implement Create, Read, Update, Delete operations.
                              Should be post, get, put, delete.
            uri:              URI on which the method is performed
            endpoint:         Server endpoint, defaults to self.repoEndpoint
            headers:          Dictionary of headers to use rather than the API-key-signed default set of headers
            retryPolicy:      A retry policy
            requests_session_async_synapse:
            kwargs:           Any other arguments taken by a
                              [request](http://docs.python-requests.org/en/latest/) method

        Returns:
            JSON encoding of response
        """
        uri, headers = self.client._build_uri_and_headers(
            uri, endpoint=endpoint, headers=headers
        )

        self.client.logger.debug(f"{method} {uri}")

        retryPolicy = self.client._build_retry_policy(retryPolicy)
        requests_session_async_synapse = (
            requests_session_async_synapse
            or self.client._requests_session_async_synapse
        )

        auth = kwargs.pop("auth", self.client.credentials)
        requests_method_fn = getattr(requests_session_async_synapse, method)
        if data:
            response = await with_retry_async(
                lambda: requests_method_fn(
                    uri,
                    data=data,
                    headers=headers,
                    auth=auth,
                    **kwargs,
                ),
                verbose=self.client.debug,
                **retryPolicy,
            )
        else:
            response = await with_retry_async(
                lambda: requests_method_fn(
                    uri,
                    headers=headers,
                    auth=auth,
                    **kwargs,
                ),
                verbose=self.client.debug,
                **retryPolicy,
            )

        self.client._handle_synapse_http_error(response)
        return response

    async def rest_get(
        self,
        uri,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session_async_synapse=None,
        **kwargs,
    ):
        """
        Sends an HTTP GET request to the Synapse server.

        Arguments:
            uri: URI on which get is performed
            endpoint: Server endpoint, defaults to self.repoEndpoint
            headers: Dictionary of headers to use rather than the API-key-signed default set of headers
            requests_session_async_synapse:
            kwargs: Any other arguments taken by a [request](http://docs.python-requests.org/en/latest/) method

        Returns:
            JSON encoding of response
        """
        try:
            with tracer.start_as_current_span("SynapseAsync::rest_get"):
                trace.get_current_span().set_attributes({"url.path": uri})
                response = await self._rest_call(
                    "get",
                    uri,
                    None,
                    endpoint,
                    headers,
                    retryPolicy,
                    requests_session_async_synapse,
                    **kwargs,
                )
                return self.client._return_rest_body(response)
        except Exception as ex:
            self.client.logger.exception(ex)

    async def rest_post(
        self,
        uri,
        body,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session_async_synapse=None,
        **kwargs,
    ):
        """
        Sends an HTTP POST request to the Synapse server.

        Arguments:
            uri: URI on which get is performed
            endpoint: Server endpoint, defaults to self.repoEndpoint
            body: The payload to be delivered
            headers: Dictionary of headers to use rather than the API-key-signed default set of headers
            requests_session_async_synapse:
            kwargs: Any other arguments taken by a [request](http://docs.python-requests.org/en/latest/) method

        Returns:
            JSON encoding of response
        """
        with tracer.start_as_current_span("SynapseAsync::rest_post"):
            trace.get_current_span().set_attributes({"url.path": uri})
            response = await self._rest_call(
                "post",
                uri,
                body,
                endpoint,
                headers,
                retryPolicy,
                requests_session_async_synapse,
                **kwargs,
            )
            return self.client._return_rest_body(response)

    async def rest_put(
        self,
        uri,
        body=None,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session_async_synapse=None,
        **kwargs,
    ):
        """
        Sends an HTTP PUT request to the Synapse server.

        Arguments:
            uri: URI on which get is performed
            endpoint: Server endpoint, defaults to self.repoEndpoint
            body: The payload to be delivered
            headers: Dictionary of headers to use rather than the API-key-signed default set of headers
            requests_session_async_synapse:
            kwargs: Any other arguments taken by a [request](http://docs.python-requests.org/en/latest/) method

        Returns
            JSON encoding of response
        """
        with tracer.start_as_current_span("SynapseAsync::rest_put"):
            trace.get_current_span().set_attributes({"url.path": uri})
            response = await self._rest_call(
                "put",
                uri,
                body,
                endpoint,
                headers,
                retryPolicy,
                requests_session_async_synapse,
                **kwargs,
            )
            return self.client._return_rest_body(response)

    async def rest_delete(
        self,
        uri,
        endpoint=None,
        headers=None,
        retryPolicy={},
        requests_session_async_synapse=None,
        **kwargs,
    ):
        """
        Sends an HTTP DELETE request to the Synapse server.

        Arguments:
            uri: URI of resource to be deleted
            endpoint: Server endpoint, defaults to self.repoEndpoint
            headers: Dictionary of headers to use rather than the API-key-signed default set of headers
            requests_session_async_synapse:
            kwargs: Any other arguments taken by a [request](http://docs.python-requests.org/en/latest/) method

        """
        with tracer.start_as_current_span("SynapseAsync::rest_delete"):
            trace.get_current_span().set_attributes({"url.path": uri})
            await self._rest_call(
                "delete",
                uri,
                None,
                endpoint,
                headers,
                retryPolicy,
                requests_session_async_synapse,
                **kwargs,
            )
