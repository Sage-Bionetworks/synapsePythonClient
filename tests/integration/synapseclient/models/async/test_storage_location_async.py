"""Integration tests for the synapseclient.models.StorageLocation class."""

import os
import uuid
from typing import Callable

import boto3
import pytest
from botocore.exceptions import ClientError
from google.cloud import storage as gcs_storage

from synapseclient import Synapse
from synapseclient.core import utils as syn_utils
from synapseclient.models import (
    File,
    Folder,
    Project,
    StorageLocation,
    StorageLocationType,
)

# External S3 bucket for integration tests
EXTERNAL_S3_BUCKET = "test-storage-location-python-client-us-east-1"

# External GCS bucket for integration tests
EXTERNAL_GCS_BUCKET = "test_storage_location_dl"
GCS_PROJECT = "sagebio-integration-testing"


class TestSynapseS3StorageLocation:
    """Integration tests for SYNAPSE_S3 storage location.

    These tests do not require external bucket credentials.
    """

    @pytest.fixture(autouse=True)
    def setup_method(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_store_and_get_synapse_s3(self) -> None:
        """Test creating and retrieving a SYNAPSE_S3 storage location."""
        storage = StorageLocation(
            storage_type=StorageLocationType.SYNAPSE_S3,
            description=f"Integration test SYNAPSE_S3 {uuid.uuid4()}",
        )

        stored = await storage.store_async(synapse_client=self.syn)

        assert stored.storage_location_id is not None
        assert stored.storage_type == StorageLocationType.SYNAPSE_S3
        assert stored.etag is not None
        assert stored.created_by is not None

        retrieved = await StorageLocation(
            storage_location_id=stored.storage_location_id
        ).get_async(synapse_client=self.syn)

        assert retrieved.storage_location_id == stored.storage_location_id
        assert retrieved.storage_type == StorageLocationType.SYNAPSE_S3

    async def test_store_synapse_s3_with_sts_enabled(self) -> None:
        """Test creating a SYNAPSE_S3 storage location with STS enabled."""
        storage = StorageLocation(
            storage_type=StorageLocationType.SYNAPSE_S3,
            sts_enabled=True,
        )

        stored = await storage.store_async(synapse_client=self.syn)

        assert stored.storage_location_id is not None
        assert stored.sts_enabled is True

        retrieved = await StorageLocation(
            storage_location_id=stored.storage_location_id
        ).get_async(synapse_client=self.syn)

        assert retrieved.sts_enabled is True

    async def test_store_is_idempotent(self) -> None:
        """Test that storing the same SYNAPSE_S3 storage location twice returns the same ID."""
        description = f"Idempotent test {uuid.uuid4()}"
        storage1 = StorageLocation(
            storage_type=StorageLocationType.SYNAPSE_S3,
            description=description,
        )
        stored1 = await storage1.store_async(synapse_client=self.syn)

        storage2 = StorageLocation(
            storage_type=StorageLocationType.SYNAPSE_S3,
            description=description,
        )
        stored2 = await storage2.store_async(synapse_client=self.syn)

        assert stored1.storage_location_id == stored2.storage_location_id


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="This test runs only locally, not in CI/CD environments.",
)
class TestExternalS3StorageLocation:
    """Integration tests for EXTERNAL_S3 storage location.

    Requires environment variables:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
      - AWS_SESSION_TOKEN

    Before each test, the bucket root object ``owner.txt`` is updated so it includes the
    current Synapse ``owner_id`` as a line if not already present (shared bucket hygiene).
    """

    @pytest.fixture(autouse=True)
    def setup_method(self, syn: Synapse, project_model: Project) -> None:
        self.syn = syn
        self.project = project_model
        self._ensure_bucket_root_owner_txt_includes_owner_id()

    def _get_s3_client(self):
        creds = {
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "aws_session_token": os.environ.get("AWS_SESSION_TOKEN"),
        }
        return boto3.client("s3", **creds)

    def _ensure_bucket_root_owner_txt_includes_owner_id(self) -> None:
        """Ensure bucket root ``owner.txt`` lists this Synapse principal (one id per line)."""
        s3 = self._get_s3_client()
        key = "owner.txt"
        owner_id = str(self.syn.credentials.owner_id)
        try:
            response = s3.get_object(Bucket=EXTERNAL_S3_BUCKET, Key=key)
            text = response["Body"].read().decode("utf-8")
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if owner_id in lines:
                return
            lines.append(owner_id)
            body = "\n".join(lines) + "\n"
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code", "")
            if code not in ("NoSuchKey", "404"):
                raise
            body = owner_id
        s3.put_object(
            Bucket=EXTERNAL_S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
        )

    def _put_owner_txt(self, base_key: str) -> None:
        """Upload owner.txt required by Synapse to validate bucket ownership."""
        s3 = self._get_s3_client()
        s3.put_object(
            Body=self.syn.credentials.owner_id,
            Bucket=EXTERNAL_S3_BUCKET,
            Key=f"{base_key}/owner.txt",
        )

    def _cleanup_prefix(self, base_key: str) -> None:
        """Delete all objects under the given prefix from the S3 bucket."""
        s3 = self._get_s3_client()
        response = s3.list_objects_v2(Bucket=EXTERNAL_S3_BUCKET, Prefix=f"{base_key}/")
        if "Contents" in response:
            s3.delete_objects(
                Bucket=EXTERNAL_S3_BUCKET,
                Delete={"Objects": [{"Key": o["Key"]} for o in response["Contents"]]},
            )

    async def test_store_and_get_external_s3(self) -> None:
        """Test creating and retrieving an EXTERNAL_S3 storage location."""
        base_key = str(uuid.uuid4())
        self._put_owner_txt(base_key)
        try:
            # GIVEN an EXTERNAL_S3 storage location
            storage = StorageLocation(
                storage_type=StorageLocationType.EXTERNAL_S3,
                bucket=EXTERNAL_S3_BUCKET,
                base_key=base_key,
                description="Integration test EXTERNAL_S3",
            )

            # WHEN we store it
            stored = await storage.store_async(synapse_client=self.syn)
            # THEN it should have the correct fields
            assert stored.storage_location_id is not None
            assert stored.storage_type == StorageLocationType.EXTERNAL_S3
            assert stored.bucket == EXTERNAL_S3_BUCKET
            assert stored.base_key == base_key

            # AND we can retrieve it by ID
            retrieved = await StorageLocation(
                storage_location_id=stored.storage_location_id
            ).get_async(synapse_client=self.syn)
            assert retrieved.storage_location_id == stored.storage_location_id
            assert retrieved.storage_type == StorageLocationType.EXTERNAL_S3
            assert retrieved.bucket == EXTERNAL_S3_BUCKET
            assert retrieved.base_key == base_key
        finally:
            self._cleanup_prefix(base_key)

    async def test_store_external_s3_and_upload_file(self) -> None:
        """Test uploading a file to an EXTERNAL_S3 storage location via Synapse."""
        base_key = str(uuid.uuid4())
        self._put_owner_txt(base_key)
        try:
            # GIVEN an EXTERNAL_S3 storage location
            stored_location = await StorageLocation(
                storage_type=StorageLocationType.EXTERNAL_S3,
                bucket=EXTERNAL_S3_BUCKET,
                base_key=base_key,
            ).store_async(synapse_client=self.syn)

            # AND a folder associated with it
            folder = await Folder(
                name=str(uuid.uuid4()),
                parent_id=self.project.id,
            ).store_async(synapse_client=self.syn)

            self.syn.setStorageLocation(
                entity=folder.id,
                storage_location_id=stored_location.storage_location_id,
            )

            # WHEN we upload a file to the folder
            upload_file = syn_utils.make_bogus_uuid_file()
            with open(upload_file, "r", encoding="utf-8") as f:
                file_contents = f.read()

            file = await File(path=upload_file, parent_id=folder.id).store_async(
                synapse_client=self.syn
            )

            # THEN the file should be downloadable and its contents match
            os.remove(upload_file)
            file = await File(id=file.id).get_async(synapse_client=self.syn)
            with open(file.path, "r", encoding="utf-8") as f:
                assert f.read() == file_contents
        finally:
            self._cleanup_prefix(base_key)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="This test runs only locally, not in CI/CD environments.",
)
class TestExternalGCSStorageLocation:
    """Integration tests for EXTERNAL_GOOGLE_CLOUD storage location.

    Requires google-cloud-storage to be installed.

    Google Cloud auth (one of):
      - Application Default Credentials: ``gcloud auth application-default login``
      - Or ``GOOGLE_APPLICATION_CREDENTIALS`` pointing at a service account JSON key

    The Storage client also needs a **project ID**. Set one of:
      - ``GOOGLE_CLOUD_PROJECT`` or ``GCLOUD_PROJECT`` (recommended for tests), or
      - ``gcloud config set project YOUR_PROJECT_ID`` so ADC resolves a default project, or
      - Use a service account key JSON that includes ``project_id`` (often inferred automatically).

    Uses bucket: test_storage_location_dl.

    Before each test, the bucket root object ``owner.txt`` is updated so it includes the
    current Synapse ``owner_id`` as a line if not already present (shared bucket hygiene).
    """

    @pytest.fixture(autouse=True)
    def setup_method(self, syn: Synapse, project_model: Project) -> None:
        self.syn = syn
        self.project = project_model
        self._ensure_bucket_root_owner_txt_includes_owner_id()

    def _gcs_client(self, project: str) -> gcs_storage.Client:
        return gcs_storage.Client(project=project)

    def _ensure_bucket_root_owner_txt_includes_owner_id(self) -> None:
        """Ensure bucket root ``owner.txt`` lists this Synapse principal (one id per line)."""
        bucket = self._gcs_client(GCS_PROJECT).bucket(EXTERNAL_GCS_BUCKET)
        blob = bucket.blob("owner.txt")
        owner_id = str(self.syn.credentials.owner_id)
        if blob.exists():
            text = blob.download_as_text(encoding="utf-8")
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            if owner_id in lines:
                return
            lines.append(owner_id)
            blob.upload_from_string("\n".join(lines) + "\n")
        else:
            blob.upload_from_string(owner_id)

    def _put_owner_txt(self, base_key: str) -> None:
        """Upload owner.txt to GCS at the base key path for Synapse bucket validation."""
        bucket = self._gcs_client(GCS_PROJECT).bucket(EXTERNAL_GCS_BUCKET)
        blob = bucket.blob(f"{base_key}/owner.txt")
        blob.upload_from_string(self.syn.credentials.owner_id)

    def _cleanup_prefix(self, base_key: str) -> None:
        bucket = self._gcs_client(GCS_PROJECT).bucket(EXTERNAL_GCS_BUCKET)
        blobs = bucket.list_blobs(prefix=f"{base_key}/")
        for blob in blobs:
            blob.delete()

    async def test_store_and_get_external_gcs(self) -> None:
        """Test creating and retrieving an EXTERNAL_GOOGLE_CLOUD storage location."""
        base_key = str(uuid.uuid4())
        self._put_owner_txt(base_key)
        try:
            storage = StorageLocation(
                storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                bucket=EXTERNAL_GCS_BUCKET,
                base_key=base_key,
                description="Integration test EXTERNAL_GOOGLE_CLOUD",
            )

            stored = await storage.store_async(synapse_client=self.syn)

            assert stored.storage_location_id is not None
            assert stored.storage_type == StorageLocationType.EXTERNAL_GOOGLE_CLOUD
            assert stored.bucket == EXTERNAL_GCS_BUCKET
            assert stored.base_key == base_key

            retrieved = await StorageLocation(
                storage_location_id=stored.storage_location_id
            ).get_async(synapse_client=self.syn)

            assert retrieved.storage_location_id == stored.storage_location_id
            assert retrieved.storage_type == StorageLocationType.EXTERNAL_GOOGLE_CLOUD
            assert retrieved.bucket == EXTERNAL_GCS_BUCKET
            assert retrieved.base_key == base_key
        finally:
            self._cleanup_prefix(base_key)

    async def test_store_external_gcs_and_upload_file(self) -> None:
        """Test uploading a file to an EXTERNAL_GOOGLE_CLOUD storage location via Synapse."""
        base_key = str(uuid.uuid4())
        self._put_owner_txt(base_key)
        try:
            storage = StorageLocation(
                storage_type=StorageLocationType.EXTERNAL_GOOGLE_CLOUD,
                bucket=EXTERNAL_GCS_BUCKET,
                base_key=base_key,
            )
            stored = await storage.store_async(synapse_client=self.syn)
            assert stored.storage_location_id is not None

            folder = await Folder(
                name=str(uuid.uuid4()),
                parent_id=self.project.id,
            ).store_async(synapse_client=self.syn)

            self.syn.setStorageLocation(
                entity=folder.id,
                storage_location_id=stored.storage_location_id,
            )

            upload_file = syn_utils.make_bogus_uuid_file()
            with open(upload_file, "r", encoding="utf-8") as f:
                file_contents = f.read()

            file = await File(path=upload_file, parent_id=folder.id).store_async(
                synapse_client=self.syn
            )

            os.remove(upload_file)
            file = await File(id=file.id).get_async(synapse_client=self.syn)
            with open(file.path, "r", encoding="utf-8") as f:
                assert f.read() == file_contents
        finally:
            self._cleanup_prefix(base_key)
