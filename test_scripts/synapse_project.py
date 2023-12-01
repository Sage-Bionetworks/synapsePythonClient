"""The purpose of this script is to demonstrate how to use the current synapse interface for projects.
The following actions are shown in this script:
1. Creating a project
2. Storing a folder to a project
3. Storing several files to a project
4. Storing several folders in a project
5. Getting metadata about a project
6. Updating the annotations in bulk for a number of folders and files
7. Deleting a project
"""
import os
import synapseclient

from synapseclient import Project, File, Annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "synapse_project"}))
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

syn = synapseclient.Synapse(debug=True)
syn.login()


def create_random_file(
    path: str,
) -> None:
    """Create a random file with random data.

    :param path: The path to create the file at.
    """
    with open(path, "wb") as f:
        f.write(os.urandom(1))


def store_project():
    # Creating annotations for my project ==================================================
    my_annotations_dict = {
        "my_key_string": ["b", "a", "c"],
        "my_key_bool": [False, False, False],
        "my_key_double": [1.2, 3.4, 5.6],
        "my_key_long": [1, 2, 3],
        "my_key_timestamp": [1701362964066, 1577862000000],
    }

    with tracer.start_as_current_span("Creating a project"):
        # Creating a project =============================================================
        project = Project(
            name="bfauble_my_new_project_for_testing_synapse_client",
            annotations=my_annotations_dict,
            description="This is a project with random data.",
        )

        my_stored_project: Project = syn.store(project)

        print(my_stored_project)

    with tracer.start_as_current_span("Storing several files to a project"):
        # Storing several files to a project =============================================
        for loop in range(1, 10):
            name_of_file = f"my_file_with_random_data_{loop}.txt"
            path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
            create_random_file(path_to_file)

            # Creating and uploading a file to a project =================================
            file = File(
                path=path_to_file,
                name=name_of_file,
                parent=my_stored_project.id,
            )
            my_stored_file = syn.store(obj=file)

            my_annotations = Annotations(
                id=my_stored_file.id,
                etag=my_stored_file.etag,
                **my_annotations_dict,
            )

            syn.set_annotations(annotations=my_annotations)

    with tracer.start_as_current_span(
        "Updating the annotations in bulk for a number of folders and files"
    ):
        # Updating the annotations in bulk for a number of folders and files =============
        new_annotations = {
            "my_key_string": ["bbbbb", "aaaaa", "ccccc"],
        }
        for child in syn.getChildren(
            parent=my_stored_project.id, includeTypes=["folder", "file"]
        ):
            is_folder = (
                "type" in child
                and child["type"] == "org.sagebionetworks.repo.model.Folder"
            )
            is_file = (
                "type" in child
                and child["type"] == "org.sagebionetworks.repo.model.FileEntity"
            )

            if is_folder:
                my_folder = syn.get(entity=child["id"])
                new_saved_annotations = syn.set_annotations(
                    Annotations(id=child["id"], etag=my_folder.etag, **new_annotations)
                )
                print(new_saved_annotations)
            elif is_file:
                my_file = syn.get(entity=child["id"], downloadFile=False)
                new_saved_annotations = syn.set_annotations(
                    Annotations(id=child["id"], etag=my_file.etag, **new_annotations)
                )
                print(new_saved_annotations)


store_project()
