from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator
from minio import Minio
import io
import json
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from typing import Union


# define the class inheriting from an existing hook class
class MinIOHook(BaseHook):
    """
    Interact with MinIO.
    :param minio_conn_id: ID of MinIO connection. (default: minio_default)
    """

    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "general"
    hook_name = "MinIOHook"

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        minio_conn_id: str = "minio_default",
        secure_connection=False,
        *args,
        **kwargs,
    ) -> None:
        # initialize the parent hook
        super().__init__(*args, **kwargs)
        # assign class variables
        self.minio_conn_id = minio_conn_id
        self.secure_connection = secure_connection
        # call the '.get_conn()' method upon initialization
        self.get_conn()

    def get_conn(self):
        """Function that initiates a new connection to MinIO."""

        conn_id = getattr(self, self.conn_name_attr)
        # get the connection object from the Airflow connection
        conn = self.get_connection(conn_id)

        client = Minio(
            conn.host,
            conn.login,
            conn.password,
            secure=self.secure_connection,
        )

        return client

    def put_object(self, bucket_name, object_name, data, length, part_size):
        """
        Write an object to Minio
        :param bucket_name: Name of the bucket.
        :param object_name: Object name in the bucket (key).
        :param data: An object having callable read() returning bytes object.
        :param length: Data size; (-1 for unknown size and set valid part_size).
        :param part_size: Multipart part size.
        """
        client = self.get_conn()
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data,
            length=length,
            part_size=part_size,
        )

    def list_objects(self, bucket_name, prefix=""):
        """
        List objects in MinIO bucket.
        :param bucket_name: Name of the bucket.
        :param prefix: optional prefix of files to list.
        """

        client = self.get_conn()
        list_of_objects = client.list_objects(bucket_name=bucket_name, prefix=prefix)
        return list_of_objects

    def copy_object(
        self, source_bucket_name, source_object_name, dest_bucket_name, dest_object_name
    ):
        """
        Copy a file from one MinIO bucket to another.
        :param source_bucket_name: Name of the source bucket.
        :param source_object_name: Name of the source file (key).
        :param dest_bucket_name: Name of the destination bucket.
        :param dest_object_name: Name of the destination file (key).
        """

        client = self.get_conn()
        copy_source = CopySource(source_bucket_name, source_object_name)
        client.copy_object(dest_bucket_name, dest_object_name, copy_source)

    def delete_objects(self, bucket_name, object_names, bypass_governance_mode=False):
        """
        Delete file(s) in a MinIO bucket.
        :param bucket_name: Name of the source bucket.
        :param object_name: Name of the source file (key).
        """
        client = self.get_conn()
        if type(object_names) == list:
            objects_to_delete = [
                DeleteObject(object_name) for object_name in object_names
            ]
        else:
            objects_to_delete = [DeleteObject(object_names)]

        errors = client.remove_objects(
            bucket_name,
            objects_to_delete,
            bypass_governance_mode=bypass_governance_mode,
        )

        for error in errors:
            self.log.info("Error occurred when deleting object:", error)


class LocalFilesystemToMinIOOperator(BaseOperator):
    """
    Operator that writes content from a local json/csv file or directly
    provided json serializeable information to MinIO.
    :param bucket_name: (required) Name of the bucket.
    :param object_name: (required) Object name in the bucket (key).
    :param local_file_path: Path to the local file that is uploaded to MinIO.
    :param json_serializeable_information: Alternatively to providing a filepath provide json-serializeable information.
    :param minio_conn_id: connection id of the MinIO connection.
    :param data: An object having callable read() returning bytes object.
    :param length: Data size; (default: -1 for unknown size and set valid part_size).
    :param part_size: Multipart part size (default: 10*1024*1024).
    """

    supported_filetypes = ["json", "csv"]
    template_fields = (
        "bucket_name",
        "object_name",
        "local_file_path",
        "json_serializeable_information",
        "minio_conn_id",
        "length",
        "part_size",
    )

    # define the .__init__() method that runs when the DAG is parsed
    def __init__(
        self,
        bucket_name,
        object_name,
        local_file_path=None,
        json_serializeable_information=None,
        minio_conn_id: str = MinIOHook.default_conn_name,
        length=-1,
        part_size=10 * 1024 * 1024,
        *args,
        **kwargs,
    ):
        # initialize the parent operator
        super().__init__(*args, **kwargs)
        # assign class variables
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.local_file_path = local_file_path
        self.json_serializeable_information = json_serializeable_information
        self.minio_conn_id = minio_conn_id
        self.length = length
        self.part_size = part_size

        if self.local_file_path:
            self.filetype = self.local_file_path.split("/")[-1].split(".")[1]
            if self.filetype not in self.supported_filetypes:
                raise AirflowException(
                    f"The LocalFilesystemToMinIOOperator currently only supports uploading the following filetypes: {self.supported_filetypes}"
                )
        elif not self.json_serializeable_information:
            raise AirflowException(
                "Provide at least one of the parameters local_file_path or json_serializeable_information"
            )

    # .execute runs when the task runs
    def execute(self, context):

        if self.local_file_path:
            if self.filetype == "csv":
                with open(self.local_file_path, "r") as f:
                    string_file = f.read()
                    data = io.BytesIO(bytes(string_file, "utf-8"))

            if self.filetype == "json":
                with open(self.local_file_path, "r") as f:
                    string_file = f.read()
                    data = io.BytesIO(bytes(json.dumps(string_file), "utf-8"))
        else:
            data = io.BytesIO(
                bytes(json.dumps(self.json_serializeable_information), "utf-8")
            )

        response = MinIOHook(self.minio_conn_id).put_object(
            bucket_name=self.bucket_name,
            object_name=self.object_name,
            data=data,
            length=self.length,
            part_size=self.part_size,
        )
        self.log.info(response)


class MinIOListOperator(BaseOperator):
    """
    List all objects from the MinIO bucket with the given string prefix in name.
    :param bucket_name: Name of the bucket.
    :param prefix: optional prefix of files to list.
    :param minio_conn_id: ID to minio connection.
    """

    def __init__(
        self,
        bucket_name,
        prefix: str = "",
        minio_conn_id: str = MinIOHook.default_conn_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.minio_conn_id = minio_conn_id

    def execute(self, context):
        list_of_objects = MinIOHook(self.minio_conn_id).list_objects(
            self.bucket_name, self.prefix
        )

        list_to_return = [obj.object_name for obj in list_of_objects]

        return list_to_return


class MinIOCopyObjectOperator(BaseOperator):
    """
    Copy files from one MinIO bucket to another.
    :param source_bucket_name: Name of the source bucket.
    :param source_object_name: Name of the source file (key).
    :param dest_bucket_name: Name of the destination bucket.
    :param dest_object_name: Name of the destination file (key).
    :param minio_conn_id: Name of the MinIO connection ID (default: minio_default)
    """

    template_fields = (
        "source_bucket_name",
        "source_object_names",
        "dest_bucket_name",
        "dest_object_names",
    )

    def __init__(
        self,
        source_bucket_name,
        source_object_names: Union[str, list],
        dest_bucket_name,
        dest_object_names: Union[str, list],
        minio_conn_id: str = MinIOHook.default_conn_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_bucket_name = source_bucket_name
        self.source_object_names = source_object_names
        self.dest_bucket_name = dest_bucket_name
        self.dest_object_names = dest_object_names
        self.minio_conn_id = minio_conn_id

        if type(self.source_object_names) != type(self.dest_object_names):
            raise AirflowException(
                "Please provide either one string each to source_object_names and dest_object_names or two lists of strings of equal length"
            )
        if type(self.source_object_names) == list and (
            len(self.source_object_names) != len(self.dest_object_names)
        ):
            raise AirflowException(
                "The lists provided to source_object_names and dest_object_names need to be of equal lenght."
            )

    def execute(self, context):
        if type(self.source_object_names) == list:
            for source_object, dest_object in zip(
                self.source_object_names, self.dest_object_names
            ):
                MinIOHook(self.minio_conn_id).copy_object(
                    self.source_bucket_name,
                    source_object,
                    self.dest_bucket_name,
                    dest_object,
                )

        else:
            MinIOHook(self.minio_conn_id).copy_object(
                self.source_bucket_name,
                self.source_object_names,
                self.dest_bucket_name,
                self.dest_object_names,
            )


class MinIODeleteObjectsOperator(BaseOperator):
    """
    Copy files from one MinIO bucket to another.
    :param bucket_name: Name of the source bucket.
    :param object_names: Name(s) of the source file(s) (key), string or list.
    """

    template_fields = (
        "bucket_name",
        "object_names",
    )

    def __init__(
        self,
        bucket_name,
        object_names: Union[str, list],
        minio_conn_id: str = MinIOHook.default_conn_name,
        bypass_governance_mode=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_names = object_names
        self.minio_conn_id = minio_conn_id
        self.bypass_governance_mode = bypass_governance_mode

    def execute(self, context):
        MinIOHook(self.minio_conn_id).delete_objects(
            self.bucket_name, self.object_names, self.bypass_governance_mode
        )
