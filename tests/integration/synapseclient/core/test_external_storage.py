"""Integration tests for external storage locations."""

import importlib
import json
import os
import tempfile
import uuid
from typing import Dict, Tuple, Any
from unittest import mock

import pytest

from synapseclient import Synapse, Folder as SynFolder
import synapseclient.core.utils as utils
from synapseclient.core.retry import with_retry
from synapseclient.models import File, Project, Folder
from synapseclient.api import (
    get_upload_destination,
)

try:
    boto3 = importlib.import_module("boto3")
except ImportError:
    boto3 = None


def check_test_preconditions() -> Tuple[bool, str]:
    """In order to run the tests in this file boto3 must be available
    and we must have configuration indicating where to store external
    storage for testing. If these conditions are not met, the tests
    will be skipped.

    Returns:
        Tuple[bool, str]: A tuple indicating whether the tests should be skipped

    """

    if not boto3:
        return True, "boto3 not available"

    elif not (
        os.environ.get("EXTERNAL_S3_BUCKET_NAME")
        and os.environ.get("EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID")
        and os.environ.get("EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY")
    ):
        return True, "External bucket access not defined in environment"

    return False, ""


def check_test_preconditions_external_object_store() -> Tuple[bool, str]:
    """In order to run the tests for an external object store we need to be able to
    authenticate directly with AWS. As such we need a profile to authenticate as.

    Returns:
        Tuple[bool, str]: A tuple indicating whether the tests should be skipped

    """

    if not (
        os.environ.get("EXTERNAL_S3_BUCKET_NAME")
        and os.environ.get("EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID")
        and os.environ.get("EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY")
    ):
        return True, "External bucket access not defined in environment"

    return False, ""


def get_aws_env() -> Tuple[str, Dict[str, str]]:
    """Get the AWS environment variables for the external storage bucket."""
    if os.environ.get("EXTERNAL_S3_BUCKET_AWS_SESSION_TOKEN"):
        return os.environ["EXTERNAL_S3_BUCKET_NAME"], {
            "aws_access_key_id": os.environ["EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ[
                "EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY"
            ],
            "aws_session_token": os.environ["EXTERNAL_S3_BUCKET_AWS_SESSION_TOKEN"],
        }

    else:
        return os.environ["EXTERNAL_S3_BUCKET_NAME"], {
            "aws_access_key_id": os.environ["EXTERNAL_S3_BUCKET_AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ[
                "EXTERNAL_S3_BUCKET_AWS_SECRET_ACCESS_KEY"
            ],
        }


skip_tests, reason = check_test_preconditions()


(
    skip_tests_external_object_store,
    reason_external_object_store,
) = check_test_preconditions_external_object_store()


@pytest.mark.skipif(skip_tests, reason=reason)
class TestExernalStorage:
    """Tests for external storage locations."""

    @pytest.fixture(autouse=True)
    def setup_method(self, syn: Synapse, project_model: Project) -> None:
        self.syn = syn
        self.project = project_model

    @classmethod
    def _make_temp_file(
        cls, contents: str = None, **kwargs
    ) -> tempfile.NamedTemporaryFile:
        # delete=False for Windows
        tmp_file = tempfile.NamedTemporaryFile(**kwargs, delete=False)
        if contents:
            with open(tmp_file.name, "w", encoding="utf-8") as f:
                f.write(contents)

        return tmp_file

    def _prepare_bucket_location(self, key_prefix: str) -> Any:
        """Create a folder in the external storage bucket and return the boto3 client.
        This also creates owner.txt required by Synapse."""
        bucket_name, aws_creds = get_aws_env()
        s3_client = boto3.client("s3", **aws_creds)
        s3_client.put_object(
            Body=self.syn.credentials.username,
            Bucket=bucket_name,
            Key=f"{key_prefix}/owner.txt",
        )

        return s3_client

    def _teardown_bucket_location(self, key_prefix: str) -> None:
        """Remove the folder and all its contents from the external storage bucket."""
        bucket_name, aws_creds = get_aws_env()
        s3_client = boto3.client("s3", **aws_creds)

        # List all objects with the prefix of the folder
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{key_prefix}/",
        )

        # Delete all objects with the prefix of the folder
        if "Contents" in response:
            objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
            s3_client.delete_objects(
                Bucket=bucket_name, Delete={"Objects": objects_to_delete}
            )

    async def _create_external_object_store(
        self, bucket_name: str, folder_name: str
    ) -> Tuple[SynFolder, Dict[str, str]]:
        folder_id = (
            await Folder(name=folder_name, parent_id=self.project.id).store_async()
        ).id

        destination = {
            "uploadType": "S3",
            "concreteType": "org.sagebionetworks.repo.model.project.ExternalObjectStorageLocationSetting",
            "endpointUrl": "https://s3.amazonaws.com",
            "bucket": bucket_name,
        }
        destination = self.syn.restPOST(
            "/storageLocation", body=json.dumps(destination)
        )

        self.syn.setStorageLocation(
            entity=folder_id,
            storage_location_id=destination["storageLocationId"],
        )

        upload_destination = await get_upload_destination(
            entity_id=folder_id, synapse_client=self.syn
        )

        return (
            self.syn.get(entity=folder_id),
            destination,
            upload_destination["keyPrefixUUID"],
        )

    async def _configure_storage_location(
        self, sts_enabled: bool = False, external_object_store: bool = False
    ) -> Tuple[Any, SynFolder, str, str]:
        folder_name = str(uuid.uuid4())
        s3_client = self._prepare_bucket_location(folder_name)

        bucket_name, _ = get_aws_env()
        try:
            if external_object_store:
                (
                    folder,
                    storage_location_setting,
                    storage_destination,
                ) = await self._create_external_object_store(
                    bucket_name=bucket_name, folder_name=folder_name
                )
                return (
                    s3_client,
                    folder,
                    storage_location_setting["storageLocationId"],
                    storage_destination,
                )
            else:
                (
                    folder,
                    storage_location_setting,
                    _,
                ) = self.syn.create_s3_storage_location(
                    parent=self.project.id,
                    folder_name=folder_name,
                    bucket_name=bucket_name,
                    base_key=folder_name,
                    sts_enabled=sts_enabled,
                )

                return (
                    s3_client,
                    folder,
                    storage_location_setting["storageLocationId"],
                    folder_name,
                )
        except Exception as ex:
            self._teardown_bucket_location(folder_name)
            raise ex

    async def test_set_external_storage_location(self) -> None:
        """Test configuring an external storage location,
        saving a file there, and confirm that it is created and
        accessible as expected."""

        # GIVEN an external storage location
        (
            s3_client,
            folder,
            _,
            folder_in_s3_to_cleanup,
        ) = await self._configure_storage_location()

        try:
            # WHEN we save a file to that location
            upload_file = utils.make_bogus_uuid_file()
            with open(upload_file, "r", encoding="utf-8") as f:
                file_contents = f.read()

            file = await File(path=upload_file, parent_id=folder.id).store_async()

            # THEN the file should be accessible via the external storage location
            os.remove(upload_file)
            file = await File(id=file.id).get_async()
            with open(file.path, "r", encoding="utf-8") as f:
                downloaded_content = f.read()
            assert file_contents == downloaded_content

            # AND that the file is readable directly via boto
            file_handle = self.syn._get_file_handle_as_creator(
                fileHandle=file.data_file_handle_id
            )

            # will raise an error if the key doesn't exist
            bucket_name, _ = get_aws_env()
            s3_client.get_object(Bucket=bucket_name, Key=file_handle["key"])
        finally:
            self._teardown_bucket_location(key_prefix=folder_in_s3_to_cleanup)

    async def test_sts_external_storage_location(self) -> None:
        """Test creating and using an external STS storage location.
        A custom storage location is created with sts enabled,
        a file is uploaded directly via boto using STS credentials,
        a file handle is created for it, and then it is read directly
        via boto using STS read credentials.
        """
        # GIVEN an external storage location with STS enabled
        bucket_name, _ = get_aws_env()
        (
            _,
            folder,
            storage_location_id,
            folder_in_s3_to_cleanup,
        ) = await self._configure_storage_location(sts_enabled=True)

        try:
            # AND credentials for reading and writing to the bucket
            sts_read_creds = self.syn.get_sts_storage_token(
                folder.id, "read_only", output_format="boto"
            )
            sts_write_creds = self.syn.get_sts_storage_token(
                folder.id, "read_write", output_format="boto"
            )

            s3_read_client = boto3.client("s3", **sts_read_creds)
            s3_write_client = boto3.client("s3", **sts_write_creds)

            # WHEN I put an object directly using the STS read only credentials
            file_contents = "saved using sts"
            temp_file = self._make_temp_file(contents=file_contents, suffix=".txt")

            remote_key = f"{folder.name}/sts_saved"

            # THEN we should not be able to write to the bucket
            with pytest.raises(Exception) as ex_cm:
                s3_read_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=bucket_name,
                    Key=remote_key,
                )
            assert "Access Denied" in str(ex_cm.value)

            # WHEN I put an object directly using the STS read/write credentials
            s3_write_client.upload_file(
                Filename=temp_file.name,
                Bucket=bucket_name,
                Key=remote_key,
                ExtraArgs={"ACL": "bucket-owner-full-control"},
            )

            # THEN I expect to be able to read from file using the read only credentials
            with_retry(
                function=lambda: s3_read_client.get_object(
                    Bucket=bucket_name, Key=remote_key
                ),
                retry_exceptions=[s3_read_client.exceptions.NoSuchKey],
            )

            # WHEN I create an external file handle for the object
            file_handle = self.syn.create_external_s3_file_handle(
                bucket_name=bucket_name,
                s3_file_key=remote_key,
                file_path=temp_file.name,
                storage_location_id=storage_location_id,
            )

            # AND store the file in Synapse
            file: File = await File(
                parent_id=folder.id, data_file_handle_id=file_handle["id"]
            ).store_async()

            # THEN I should be able to retrieve the file via synapse
            retrieved_file_entity = await File(id=file.id).get_async()
            with open(retrieved_file_entity.path, "r", encoding="utf-8") as f:
                assert file_contents == f.read()
        finally:
            self._teardown_bucket_location(key_prefix=folder_in_s3_to_cleanup)

    async def test_boto_upload_acl(self) -> None:
        """Verify when we store a Synapse object using boto we apply a
        bucket-owner-full-control ACL to the object"""
        # GIVEN an external storage location with STS enabled
        bucket_name, _ = get_aws_env()
        _, folder, _, folder_in_s3_to_cleanup = await self._configure_storage_location(
            sts_enabled=True
        )

        try:
            file_contents = str(uuid.uuid4())
            upload_file = self._make_temp_file(contents=file_contents)

            # WHEN I upload a file using boto sts transfer
            with mock.patch.object(
                self.syn,
                "use_boto_sts_transfers",
                new_callable=mock.PropertyMock(return_value=True),
            ):
                file = await File(
                    path=upload_file.name, parent_id=folder.id
                ).store_async()
                assert os.path.exists(file.path)
                os.remove(file.path)

                # THEN I should be able to donwload the file
                assert not os.path.exists(file.path)
                file_copy = await File(
                    id=file.id, download_location=os.path.dirname(upload_file.name)
                ).get_async()
                assert os.path.exists(file_copy.path)
                assert file_copy.path == upload_file.name

            # AND the file should be accessible via the external storage location
            s3_read_client = boto3.client("s3", **get_aws_env()[1])
            bucket_acl = s3_read_client.get_bucket_acl(Bucket=bucket_name)
            bucket_grantee = bucket_acl["Grants"][0]["Grantee"]
            assert bucket_grantee["Type"] == "CanonicalUser"
            bucket_owner_id = bucket_grantee["ID"]

            # with_retry to avoid acidity issues of an S3 put
            object_acl = with_retry(
                function=lambda: s3_read_client.get_object_acl(
                    Bucket=bucket_name, Key=file.file_handle.key
                ),
                retry_exceptions=[s3_read_client.exceptions.NoSuchKey],
            )

            # AND the object should have the bucket owner as the grantee
            grants = object_acl["Grants"]
            assert len(grants) == 1
            grant = grants[0]
            grantee = grant["Grantee"]
            assert grantee["Type"] == "CanonicalUser"
            assert grantee["ID"] == bucket_owner_id
            assert grant["Permission"] == "FULL_CONTROL"
        finally:
            self._teardown_bucket_location(key_prefix=folder_in_s3_to_cleanup)

    @pytest.mark.skipif(
        skip_tests_external_object_store, reason=reason_external_object_store
    )
    async def test_external_object_store(self) -> None:
        """Test configuring an external object storage location,
        saving a file there, and confirm that it is created and
        accessible as expected."""

        # GIVEN an external object storage location
        (
            s3_client,
            folder,
            _,
            folder_in_s3_to_cleanup,
        ) = await self._configure_storage_location(external_object_store=True)

        try:
            with mock.patch(
                "synapseclient.core.upload.upload_functions_async._get_aws_credentials",
                return_value=get_aws_env()[1],
            ), mock.patch(
                "synapseclient.core.download.download_functions._get_aws_credentials",
                return_value=get_aws_env()[1],
            ):
                # WHEN we save a file to that location
                upload_file = utils.make_bogus_uuid_file()
                with open(upload_file, "r", encoding="utf-8") as f:
                    file_contents = f.read()

                file = await File(path=upload_file, parent_id=folder.id).store_async()

                # THEN the file should be accessible via the external storage location
                os.remove(upload_file)
                file = await File(id=file.id).get_async()
                with open(file.path, "r", encoding="utf-8") as f:
                    downloaded_content = f.read()
                assert file_contents == downloaded_content

                # AND that the file is readable directly via boto
                file_handle = self.syn._get_file_handle_as_creator(
                    fileHandle=file.data_file_handle_id
                )

                # will raise an error if the key doesn't exist
                bucket_name, _ = get_aws_env()
                s3_client.get_object(Bucket=bucket_name, Key=file_handle["fileKey"])
        finally:
            self._teardown_bucket_location(key_prefix=folder.name)
            self._teardown_bucket_location(key_prefix=folder_in_s3_to_cleanup)
