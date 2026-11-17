# Upload Resume

A large upload can stop before it is complete: the network connection fails, the
computer goes to sleep, or the process stops. This page explains when the Synapse
Python client can continue such an upload from the parts that the storage already
has, and when it must send the full file again.

For the steps, see [Resume an upload that stopped](../guides/data_storage.md#resume-an-upload-that-stopped)
and [Resume a direct S3 upload with STS credentials](../guides/data_storage.md#resume-a-direct-s3-upload-with-sts-credentials).

## How the client resumes an upload

Uploads to Synapse S3 storage, external S3 storage, and external Google Cloud
storage use the Synapse multipart upload API. To start or find an upload, the
client sends [`POST /file/multipart`](https://rest-docs.synapse.org/rest/POST/file/multipart.html)
with a description of the file. The response is a
[`MultipartUploadStatus`](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/MultipartUploadStatus.html)
with an `uploadId` and a `partsState` string. `partsState` has one character for
each part: `1` if Synapse has the part, and `0` if it does not. The client sends
only the parts that have a `0`. It adds each part with
[`PUT /file/multipart/{uploadId}/add/{partNumber}`](https://rest-docs.synapse.org/rest/PUT/file/multipart/uploadId/add/partNumber.html),
and finishes with
[`PUT /file/multipart/{uploadId}/complete`](https://rest-docs.synapse.org/rest/PUT/file/multipart/uploadId/complete.html).

Synapse identifies an upload by the full request (the file MD5, name, size,
content type, part size, and destination storage location) and the user who sends
it. If an upload that is not complete has the same request and the same user,
Synapse returns that upload. Thus, if you run the same
[File.store][synapseclient.models.File.store] or
[`synapse store`](../tutorials/command_line_client.md#store) command again, the
client continues the earlier upload. If the earlier upload completed and its file
handle still exists, Synapse returns the file handle and the client sends no parts.

## What must stay the same

To resume, all of these must be the same as in the run that stopped:

* The Synapse user.
* The file content. A change to the content changes the MD5.
* The file name and the destination folder or storage location. The client
  calculates the part size from the file size, so it does not change.

Synapse deletes an upload that is not complete 30 days after a part was last
added to it. A run that adds no parts does not reset this time. After that, the
upload starts again from byte 0.

## Network failures and sleep

Each part retries a connection failure or a server error until the total wait
between retries is about 20 minutes. A part upload URL is valid for 15 minutes.
If a URL expires, the client gets new URLs one time for that part. If a part
still fails, the client tries the full upload again. The client makes up to 8
attempts in one call, and each attempt resumes from the parts that Synapse has.

Thus, a short network failure or sleep usually does not stop the upload. If the
call fails and a second run can continue the upload, the error has a
[note](https://docs.python.org/3/library/exceptions.html#BaseException.add_note)
that tells you to run the same command again. Errors that a second run cannot fix,
for example a permission error (HTTP 403) or an incorrect request (HTTP 400), do
not have the note. Copies do not have the note.

## Lost uploads

On external S3 storage, an
[`AbortIncompleteMultipartUpload` lifecycle rule](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html)
can delete the uploaded parts before Synapse deletes its record of the upload.
Synapse does not ask S3 which parts it has when it returns `partsState`, so it
returns the same `uploadId` as before. The S3 error `NoSuchUpload` shows only when
the client adds the next part (the add-part response has the state `ADD_FAILED`)
or completes the upload (HTTP 400).

When this occurs, the client logs a warning, asks Synapse for a new upload with
`forceRestart=true`, and sends all parts again. It does this one time in each
call. This automatic restart applies only to S3. It is not available for Google
Cloud storage, and not in the deprecated synchronous upload functions.

## The MD5 before a resume

The MD5 of the file is a part of the request that identifies the upload. Thus,
before the client can find the earlier upload, it must read the full file again
to calculate the MD5. For a very large file, this read takes some time before the
first part is sent, but it is usually much shorter than the upload itself.

## Paths that cannot resume

These upload paths send the full file again after a stop:

* **Automatic boto3 STS transfers**
  ([`use_boto_sts_transfers`](../guides/data_storage.md#automatic-transfersto-from-sts-storage-locations-using-boto3-with-synapseclient)).
  Each upload uses a new random S3 key, and boto3
  [`upload_file`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html)
  aborts the S3 multipart upload when it fails. If you set
  `use_boto_sts_transfers` to `False`, uploads to an STS-enabled folder use the
  Synapse multipart upload API, which can resume.
* **[SFTP](../guides/data_storage.md#sftp)** storage locations.
* **Client-authenticated S3-compatible** storage locations (external object
  stores).

## Direct S3 uploads with STS credentials

This section applies only when you upload directly to S3 with STS credentials and
AWS tools. Uploads with the Synapse client resume automatically, as the sections
above tell.

With STS credentials from
[Folder.get_sts_storage_token][synapseclient.models.Folder.get_sts_storage_token],
you can upload to an STS-enabled folder with AWS tools instead of the Synapse
client. The common AWS tools use an
[S3 multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
for large files, but they do not resume after a restart:

| Tool | Resumes after a restart? |
|---|---|
| [`aws s3 cp`](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/cp.html) / [`aws s3 sync`](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/sync.html) | No. If a multipart upload fails, it aborts the upload. A later run starts from byte 0. |
| boto3 [`upload_file`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html) / [`upload_fileobj`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_fileobj.html) | No. These use the same transfer code as the AWS CLI, which aborts the upload when it fails. |
| boto3 low-level multipart calls ([`create_multipart_upload`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_multipart_upload.html), [`upload_part`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_part.html), [`list_parts`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/paginator/ListParts.html), [`complete_multipart_upload`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/complete_multipart_upload.html)) | Yes, if you keep the `UploadId` in a local file. The [how-to guide](../guides/data_storage.md#resume-a-direct-s3-upload-with-sts-credentials) has a full script. |

Two limits apply to the low-level procedure:

* STS credentials are valid for 12 hours by default
  ([`GET /entity/{id}/sts`](https://rest-docs.synapse.org/rest/GET/entity/id/sts.html)).
  The `expiration` field of the credentials gives the exact time. For a long
  upload, get new credentials before they expire, and continue with the same
  `UploadId`.
* If the storage location has a base key, the STS policy does not let you list the
  multipart uploads in the bucket. Thus, save the `UploadId` yourself when you
  start the upload.

The bucket owner can also set an `AbortIncompleteMultipartUpload` lifecycle rule.
That rule sets the time limit for a resume.
