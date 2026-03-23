import boto3


def upload_file(local_path, bucket, key):
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)