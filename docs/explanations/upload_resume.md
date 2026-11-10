# Upload Resume

A large upload can stop before it is complete: the network connection fails, the
computer goes to sleep, or the process stops. This page explains when the Synapse
Python client can continue such an upload from the parts that the storage already
has, and when it must send the full file again.

For the steps, see [Resume an upload that stopped](../guides/data_storage.md#resume-an-upload-that-stopped)
and [Resume a direct S3 upload with STS credentials](../guides/data_storage.md#resume-a-direct-s3-upload-with-sts-credentials).

## How the client resumes an upload

Uploads to Synapse S3 storage, external S3 storage, and external Google Cloud
storage use the Synapse multipart upload API. The client sends
[`POST /file/multipart`](https://rest-docs.synapse.org/rest/POST/file/multipart.html)
with a description of the file. The response has an `uploadId` and a
`partsState` string, with one `0` or `1` for each part. The client then sends only
the parts that have a `0`.

The server compares the full request (the file MD5, name, size, content type,
part size, and destination storage location) and the user who sends it. If an
upload that is not complete has the same request and the same user, the server
returns that upload. Thus, if you run the same `File.store()` or `synapse store`
command again, the client continues the earlier upload. If the earlier upload
completed and its file handle still exists, the server returns the file handle and
the client sends no parts.

## What must stay the same

To resume, all of these must be the same as in the run that stopped:

* The Synapse user.
* The file content. A change to the content changes the MD5.
* The file name, the destination folder or storage location, and the part size.

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
call fails, the error has a note that tells you to run the same command again.

## Lost uploads

On external S3 storage, a bucket lifecycle rule such as
`AbortIncompleteMultipartUpload` can delete the uploaded parts before Synapse
deletes its record of the upload. Then Synapse reports the S3 error `NoSuchUpload`
when the client adds the next part or completes the upload. When this occurs, the client logs a warning, starts a
new upload, and sends all parts again. It does this one time in each call.

This automatic restart applies only to S3. It is not available for Google Cloud
storage, and not in the deprecated synchronous upload functions.

## The MD5 before a resume

The MD5 of the file is a part of the request that identifies the upload. Thus,
before the client can find the earlier upload, it must read the full file again
to calculate the MD5. For a very large file, this read takes some time before the
first part is sent, but it is usually much shorter than the upload itself.

## Paths that cannot resume

These upload paths send the full file again after a stop:

* **Automatic boto3 STS transfers** (`use_boto_sts_transfers`). Each upload uses
  a new random S3 key, and boto3 `upload_file` aborts the S3 multipart upload when
  it fails.
* **SFTP** storage locations.
* **Client-authenticated S3-compatible** storage locations (external object
  stores).

## Direct S3 uploads with STS credentials

With STS credentials, you can upload to an STS-enabled folder with AWS tools
instead of the Synapse client. The common AWS tools do not resume after a restart:

| Tool | Resumes after a restart? |
|---|---|
| `aws s3 cp` / `aws s3 sync` | No. If a multipart upload fails, it aborts the upload. A later run starts from byte 0. |
| boto3 `upload_file` / `upload_fileobj` | No. These use the same transfer code as the AWS CLI, which aborts the upload when it fails. |
| boto3 low-level multipart calls | Yes, if you keep the `UploadId` in a local file. See the [how-to guide](../guides/data_storage.md#resume-a-direct-s3-upload-with-sts-credentials). |

Two limits apply to the low-level procedure:

* STS credentials are valid for 12 hours by default. The `expiration` field of
  the credentials gives the exact time. For a long upload, get new credentials
  when S3 returns `ExpiredToken`, and continue.
* If the storage location has a base key, the STS policy does not let you list the
  multipart uploads in the bucket. Thus, save the `UploadId` yourself when you
  start the upload.

The bucket owner can also set an `AbortIncompleteMultipartUpload` lifecycle rule.
That rule sets the time limit for a resume.
