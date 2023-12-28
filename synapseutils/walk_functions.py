from synapseclient.entity import is_container
import os


def walk(
    syn,
    synId,
    includeTypes=[
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
    Traverse through the hierarchy of files and folders stored under the synId. Has the same behavior as os.walk()

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

        The output will look like this:

            Directory (syn12345678): My Project Name/my_directory_name
            File (syn23456789): My Project Name/my_directory_name/fileA.txt

    Example: Using this function
        Traversing through a project and printing out the directory path, folders, and files

            walkedPath = walk(syn, "syn1234", ["file"]) #Exclude tables and views

            for dirpath, dirname, filename in walkedPath:
                print(dirpath)
                print(dirname) #All the folders in the directory path
                print(filename) #All the files in the directory path

    """
    # Ensure that "folder" is included so the hierarchy can be traversed
    if "folder" not in includeTypes:
        includeTypes.append("folder")
    return _helpWalk(syn, synId, includeTypes)


# Helper function to hide the newpath parameter
def _helpWalk(syn, synId, includeTypes, newpath=None):
    """Helper function that helps build the directory path per result by
    traversing through the hierarchy of files and folders stored under the synId.
    Has the same behavior as os.walk()

    :param syn:     A synapse object: syn = synapseclient.login()- Must be logged into synapse
    :param synId:   A synapse ID of a folder or project
    :param includeTypes:    Must be a list of entity types (ie. ["file", "table"]) which can be found here:
                            http://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/EntityType.html
                            The "folder" type is always included so the hierarchy can be traversed
    :param newpath: The directory path of the listed files
    """
    starting = syn.get(synId, downloadFile=False)
    # If the first file is not a container, return immediately
    if newpath is None and not is_container(starting):
        return
    elif newpath is None:
        dirpath = (starting["name"], synId)
    else:
        dirpath = (newpath, synId)
    dirs = []
    nondirs = []
    results = syn.getChildren(synId, includeTypes)
    for i in results:
        if is_container(i):
            dirs.append((i["name"], i["id"]))
        else:
            nondirs.append((i["name"], i["id"]))
    yield dirpath, dirs, nondirs
    for name in dirs:
        # The directory path for each os.walk() result needs to be built up
        # This is why newpath is passed in
        newpath = os.path.join(dirpath[0], name[0])
        for x in _helpWalk(syn, name[1], includeTypes, newpath=newpath):
            yield x
