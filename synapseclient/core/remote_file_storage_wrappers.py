from contextlib import contextmanager
import os
import time
import multiprocessing
import urllib.parse as urllib_parse

from synapseclient.core.retry import with_retry
from synapseclient.core.cumulative_transfer_progress import printTransferProgress
from synapseclient.core.utils import attempt_import


class S3ClientWrapper:
    # These methods are static because in our use case, we always have the bucket and
    # endpoint and usually only call the download/upload once so there is no need to instantiate multiple objects

    @staticmethod
    def _attempt_import_boto3():
        """
        Check if boto3 installed and give instructions if not.
        """
        return attempt_import(
            "boto3",
            "\n\nLibraries required for client authenticated S3 access are not installed!\n"
            "The Synapse client uses boto3 in order to access S3-like storage "
            "locations.\n",
        )

    @staticmethod
    def _create_progress_callback_func(file_size, filename, prefix=None):
        bytes_transferred = multiprocessing.Value("d", 0)
        t0 = time.time()

        def progress_callback(bytes):
            with bytes_transferred.get_lock():
                bytes_transferred.value += bytes
                printTransferProgress(
                    bytes_transferred.value,
                    file_size,
                    prefix=prefix,
                    postfix=filename,
                    dt=time.time() - t0,
                    previouslyTransferred=0,
                )

        return progress_callback

    @staticmethod
    def download_file(
        bucket,
        endpoint_url,
        remote_file_key,
        download_file_path,
        *,
        profile_name=None,
        credentials=None,
        show_progress=True,
        transfer_config_kwargs=None,
    ):
        """
        Download a file from s3 using boto3.

        :param bucket:                  name of bucket to upload to
        :param endpoint_url:            a boto3 compatible endpoint url
        :param remote_file_key:         object key to upload the file to
        :param download_file_path:      local path to save the file to
        :param profile_name:            AWS profile name from local aws config, mutually exclusive with credentials
        :param credentials:             a dictionary of AWS credentials to use, mutually exclusive with profile_name
                                        (aws_access_key_id, aws_secret_access_key, aws_session_token)
        :param show_progress:           whether to print progress indicator to console
        :param transfer_config_kwargs:  boto S3 transfer configuration (see boto3.s3.transfer.TransferConfig)

        """

        S3ClientWrapper._attempt_import_boto3()

        import botocore
        import boto3.s3.transfer

        transfer_config = boto3.s3.transfer.TransferConfig(
            **(transfer_config_kwargs or {})
        )

        session_args = credentials if credentials else {"profile_name": profile_name}
        boto_session = boto3.session.Session(**session_args)
        s3 = boto_session.resource("s3", endpoint_url=endpoint_url)

        try:
            s3_obj = s3.Object(bucket, remote_file_key)

            progress_callback = None
            if show_progress:
                s3_obj.load()
                file_size = s3_obj.content_length
                filename = os.path.basename(download_file_path)
                progress_callback = S3ClientWrapper._create_progress_callback_func(
                    file_size,
                    filename,
                    prefix="Downloading",
                )

            s3_obj.download_file(
                download_file_path,
                Callback=progress_callback,
                Config=transfer_config,
            )

            # why return what we were passed...?
            return download_file_path

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise ValueError(
                    "The key:%s does not exist in bucket:%s.", remote_file_key, bucket
                )
            else:
                raise

    @staticmethod
    def upload_file(
        bucket,
        endpoint_url,
        remote_file_key,
        upload_file_path,
        *,
        profile_name=None,
        credentials=None,
        show_progress=True,
        transfer_config_kwargs=None,
    ):
        """
        Upload a file to s3 using boto3.

        :param bucket:                  name of bucket to upload to
        :param endpoint_url:            a boto3 compatible endpoint url
        :param remote_file_key:         object key to upload the file to
        :param upload_file_path:        local path of the file to upload
        :param profile_name:            AWS profile name from local aws config, mutually exclusive with credentials
        :param credentials:             a dictionary of AWS credentials to use, mutually exclusive with profile_name
                                        (aws_access_key_id, aws_secret_access_key, aws_session_token)
        :param show_progress:           whether to print progress indicator to console
        :param transfer_config_kwargs:  boto S3 transfer configuration (see boto3.s3.transfer.TransferConfig)
        """

        if not os.path.isfile(upload_file_path):
            raise ValueError(
                "The path: [%s] does not exist or is not a file", upload_file_path
            )

        S3ClientWrapper._attempt_import_boto3()
        import boto3.s3.transfer

        transfer_config = boto3.s3.transfer.TransferConfig(
            **(transfer_config_kwargs or {})
        )

        session_args = credentials if credentials else {"profile_name": profile_name}
        boto_session = boto3.session.Session(**session_args)
        s3 = boto_session.resource("s3", endpoint_url=endpoint_url)

        progress_callback = None
        if show_progress:
            file_size = os.stat(upload_file_path).st_size
            filename = os.path.basename(upload_file_path)
            progress_callback = S3ClientWrapper._create_progress_callback_func(
                file_size, filename, prefix="Uploading"
            )

        # automatically determines whether to perform multi-part upload
        s3.Bucket(bucket).upload_file(
            upload_file_path,
            remote_file_key,
            Callback=progress_callback,
            Config=transfer_config,
            ExtraArgs={"ACL": "bucket-owner-full-control"},
        )
        return upload_file_path


class SFTPWrapper:
    @staticmethod
    def _attempt_import_sftp():
        """
        Check if pysftp is installed and give instructions if not.
        :return: the pysftp module if available
        """
        return attempt_import(
            "pysftp",
            "\n\nLibraries required for SFTP are not installed!\n"
            "The Synapse client uses pysftp in order to access SFTP storage locations. "
            "This library in turn depends on pycrypto.\n"
            "For Windows systems without a C/C++ compiler, install the appropriate binary "
            "distribution of pycrypto from:\n"
            "http://www.voidspace.org.uk/python/modules.shtml#pycrypto\n\n"
            "For more information, see: http://python-docs.synapse.org/build/html/sftp.html",
        )

    @staticmethod
    def _parse_for_sftp(url):
        parsedURL = urllib_parse.urlparse(url)
        if parsedURL.scheme != "sftp":
            raise (
                NotImplementedError(
                    "This method only supports sftp URLs of the form sftp://..."
                )
            )
        return parsedURL

    @staticmethod
    def upload_file(filepath, url, username=None, password=None):
        """
        Performs upload of a local file to an sftp server.

        :param filepath:    The file to be uploaded
        :param url:         URL where file will be deposited. Should include path and protocol. e.g.
                            sftp://sftp.example.com/path/to/file/store
        :param username:    username on sftp server
        :param password:    password for authentication on the sftp server

        :returns: A URL where file is stored
        """
        parsedURL = SFTPWrapper._parse_for_sftp(url)
        with _retry_pysftp_connection(
            parsedURL.hostname, username=username, password=password
        ) as sftp:
            sftp.makedirs(parsedURL.path)
            with sftp.cd(parsedURL.path):
                sftp.put(filepath, preserve_mtime=True, callback=printTransferProgress)

        path = urllib_parse.quote(parsedURL.path + "/" + os.path.split(filepath)[-1])
        parsedURL = parsedURL._replace(path=path)
        return urllib_parse.urlunparse(parsedURL)

    @staticmethod
    def download_file(
        url, localFilepath=None, username=None, password=None, show_progress=True
    ):
        """
        Performs download of a file from an sftp server.

        :param url:             URL where file will be deposited.  Path will be chopped out.
        :param localFilepath:   location where to store file
        :param username:        username on server
        :param password:        password for authentication on  server
        :param show_progress:   whether to print progress indicator to console

        :returns: localFilePath

        """
        parsedURL = SFTPWrapper._parse_for_sftp(url)

        # Create the local file path if it doesn't exist
        path = urllib_parse.unquote(parsedURL.path)
        if localFilepath is None:
            localFilepath = os.getcwd()
        if os.path.isdir(localFilepath):
            localFilepath = os.path.join(localFilepath, path.split("/")[-1])
        # Check and create the directory
        dir = os.path.dirname(localFilepath)
        if not os.path.exists(dir):
            os.makedirs(dir)

        # Download file
        with _retry_pysftp_connection(
            parsedURL.hostname, username=username, password=password
        ) as sftp:
            sftp.get(
                path,
                localFilepath,
                preserve_mtime=True,
                callback=(printTransferProgress if show_progress else None),
            )
        return localFilepath


@contextmanager
def _retry_pysftp_connection(*conn_args, **conn_kwargs):
    pysftp = SFTPWrapper._attempt_import_sftp()

    # handle error reading banner which can mean an overloaded SSH server,
    # especially in the context of our integration tests if there are multiple concurrent
    # test suites running aginst the test micro instance
    # https://stackoverflow.com/a/29225295
    sftp = with_retry(
        lambda: pysftp.Connection(*conn_args, **conn_kwargs),
        retry_errors=["Error reading SSH protocol banner"],
    )
    try:
        yield sftp
    finally:
        sftp.close()
