import os
from typing import Any, Callable, Dict, List

"""
This class provides utility functions for working with AWS S3.
Attributes:
    bucket_name (str): The name of the S3 bucket.
    input_prefix (str): The prefix for input objects in the S3 bucket.
    output_prefix (str): The prefix for output objects in the S3 bucket.
    s3_client (S3Client): An instance of the S3 client.
Methods:
    list_objects(): Lists objects in the S3 bucket with the specified input prefix.
    get_contents(): Gets the contents of the S3 bucket from a `list_objects()` call.
    send(body, key, encode=None): Sends an object to the S3 bucket with the specified output prefix.
    get(key): Retrieves an object from the S3 bucket with the specified key.
    get_body(key, decode=None): Retrieves the body of an object from the S3 bucket with the specified key.
"""


class S3Mailman:
    def __init__(self, bucket_name: str, input_prefix: str, output_prefix: str, s3_client) -> None:
        self.bucket_name = bucket_name
        self.input_prefix = input_prefix
        self.output_prefix = output_prefix
        self.s3_client = s3_client

    def list_objects(self) -> Dict:
        return self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.input_prefix)

    def get_contents(self) -> List:
        return self.list_objects().get("Contents", [])

    def send(self, body: Any, key, encode=None) -> None:
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=os.path.join(self.output_prefix, key),
            Body=body if encode is None else body.encode(encode),
        )

    def send_all(
        self,
        objects: List,
        base_path: str,
        encode: str | None = None,
        preprocess: Callable | None = None,
    ) -> None:
        for i, body in enumerate(objects):
            body_processed = body if preprocess is None else preprocess(body)
            self.send(body_processed, os.path.join(base_path, str(i + 1)), encode=encode)

    def get(self, key: str) -> Dict:
        return self.s3_client.get_object(Bucket=self.bucket_name, Key=key)

    def get_body(self, key: str, decode: str | None = None) -> Any:
        return (
            self.get(key)["Body"].read().decode(decode)
            if decode is not None
            else self.get(key)["Body"].read()
        )
