#!/usr/bin/env python3
import boto3
import os

bucket = "ice-glue-test-01"
prefix = "warehouse/"

# Read AWS credentials from environment
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

if not access_key or not secret_key:
    print("ERROR: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not set")
    exit(1)

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region,
)

print(f"Listing objects in s3://{bucket}/{prefix} ...")
paginator = s3.get_paginator("list_objects_v2")
objects = []
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    if "Contents" in page:
        for obj in page["Contents"]:
            print(f"  Delete: {obj['Key']} ({obj['Size']} bytes)")
            objects.append({"Key": obj["Key"]})

if not objects:
    print("No objects found.")
else:
    print(f"Deleting {len(objects)} objects ...")
    response = s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    if "Deleted" in response:
        print(f"Deleted {len(response['Deleted'])} objects.")
    if "Errors" in response:
        for err in response["Errors"]:
            print(f"  ERROR: {err['Key']} - {err['Message']}")

print("Done.")
