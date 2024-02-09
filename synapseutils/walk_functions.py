import os
import typing

import synapseclient
from synapseclient.entity import is_container


def walk(
    syn: synapseclient.Synapse,
    synId: str,
    includeTypes: typing.List[str] = [
        "folder",
        "file",
        "table",
        "link",
        "entityview",
        "dockerrepo",
        "submissionview",
        "dataset",
        "materializedview",
    ],
):
    """
    Traverse through the hierarchy of files and folders stored under the synId.
    Has the same behavior as os.walk()

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        synId: A synapse ID of a folder or project
        includeTypes: Must be a list of entity types (ie.["file", "table"])
                        The "folder" type is always included so the hierarchy can be traversed

    Example: Print Project & Files in slash delimited format
        Traversing through a project and print out each Folder and File

            import synapseclient
            import synapseutils
            syn = synapseclient.login()

            for directory_path, directory_names, file_name in synapseutils.walk(
                syn=syn, synId="syn1234", includeTypes=["file"]
            ):
                for directory_name in directory_names:
                    print(
                        f"Directory ({directory_name[1]}): {directory_path[0]}/{directory_name[0]}"
                    )

                for file in file_name:
                    print(f"File ({file[1]}): {directory_path[0]}/{file[0]}")

        The output will look like this assuming only 1 folder and 1 file in the directory:

            Directory (syn12345678): My Project Name/my_directory_name
            File (syn23456789): My Project Name/my_directory_name/fileA.txt

    Example: Using this function
        Traversing through a project and printing out the directory path, folders, and files

            walkedPath = walk(syn, "syn1234", ["file"]) #Exclude tables and views

            for dirpath, dirname, filename in walkedPath:
                print(dirpath)
                print(dirname) #All the folders in the directory path
                print(filename) #All the files in the directory path

    This is a high level sequence diagram of the walk function:

    ```mermaid
    sequenceDiagram
        autonumber
        participant walk

        opt Not start_entity
            walk->>client: Call `.get()` method
            client-->>walk: Metadata about the root start_entity
        end

        alt Root is not a container
            note over walk: Return early
        else newpath is none
            note over walk: Get directory path from name of entity and synapse ID
        else
            note over walk: Use path passed in from recursive call
        end

        loop Get children for container
            walk->>client: Call `.getChildren()` method
            client-->>walk: return immediate children
            note over walk: Aggregation of all children into dirs and non-dirs list
        end

        loop For each directory
            walk->>walk: Recursively call walk
        end
    ```
    """
    # Ensure that "folder" is included so the hierarchy can be traversed
    if "folder" not in includeTypes:
        includeTypes.append("folder")
    return _help_walk(syn=syn, syn_id=synId, include_types=includeTypes)


# Helper function to hide the newpath parameter
def _help_walk(
    syn: synapseclient.Synapse,
    syn_id: str,
    include_types: typing.List[str],
    start_entity: typing.Union[synapseclient.Entity, dict] = None,
    newpath: str = None,
):
    """Helper function that helps build the directory path per result by
    traversing through the hierarchy of files and folders stored under the synId.
    Has the same behavior as os.walk()

    Arguments:
        syn: A synapse object: syn = synapseclient.login()- Must be logged into synapse
        syn_id: A synapse ID of a folder or project
        include_types: Must be a list of entity types (ie. ["file", "table"]) which can be found here:
                    http://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html
                    The "folder" type is always included so the hierarchy can be traversed.
                    This was converted from a list to a tuple to enable caching.
        start_entity: A Synapse entity or Synapse entity represented as a dictionary. Typical use is to pass in
                    a synapseclient.Entity first and then recursive calls will be made with dictionaries representing
                    the first entity's children. Defaults to None.
        newpath: The directory path of the listed files. Defaults to None.
    """
    if not start_entity:
        start_entity = syn.get(syn_id, downloadFile=False)
    if newpath is None and not is_container(start_entity):
        return
    elif newpath is None:
        dirpath = (start_entity["name"], syn_id)
    else:
        dirpath = (newpath, syn_id)
    results = syn.getChildren(syn_id, include_types)
    dirs = []
    dir_entities = []
    nondirs = []
    for i in results:
        if is_container(i):
            dirs.append((i["name"], i["id"]))
            dir_entities.append(i)
        else:
            nondirs.append((i["name"], i["id"]))
    yield dirpath, dirs, nondirs
    for i, name in enumerate(dirs):
        # The directory path for each os.walk() result needs to be built up.
        # This is why newpath is passed in.
        newpath = os.path.join(dirpath[0], name[0])
        # pass the appropriate entity dictionary to the recursive call
        next_entity = dir_entities[i]
        for x in _help_walk(
            syn=syn,
            syn_id=name[1],
            include_types=include_types,
            start_entity=next_entity,
            newpath=newpath,
        ):
            yield x
