import json
import os.path

from pymongo import MongoClient
from botocore.exceptions import ClientError
import boto3
import logging

CONNECTION_STRING = 'mongodb+srv://mongoadmin:Cij3mPHBEvsWr5Jw' \
                    '@cluster0.dpliugt.mongodb.net/?retryWrites=true&w=majority'
DB_NAME = 'Jaarkesh'
COL_NAME = 'Promotion'

"""
connect()
TCP/IP connection to DBaas
"""


def get_db_client():
    client = MongoClient(CONNECTION_STRING)
    return client[DB_NAME]


def shutdown_db_client(client):
    client.close()


def get_promotion_col(db):
    return db[COL_NAME]


def get_promotions():
    db = get_db_client()
    promo_col = get_promotion_col(db)

    return promo_col


"""
CRUD operations for Promotions collection.
Create, Read, Update, Delete
"""

"""
Create
"""


def insert(col, pdict):
    _id = col.insert_one(pdict)

    return _id


def insert_many(col, pdicts):
    _idx = col.insert_many(pdicts)

    return _idx


"""
Read
"""


def find(col, query):
    return col.find(query)


def find_limit(col, query, limit):
    return col.find(query).limit(limit)


def find_all(col):
    return col.find()


"""
Update
"""


def update(col, query, nvals):
    col.update_one(query, nvals)


def update_many(col, query, nvals):
    col.update_many(query, nvals)


"""
Delete
"""


def delete(col, query):
    col.delete_one(query)


def delete_many(col, query):
    col.delete_many(query)


def delete_all(col):
    col.delete_many({})


"""
Arvan Cloud S3 storage
"""

ENDPOINT_URL = 'https://s3.ir-thr-at1.arvanstorage.com'
ACCESS_KEY = '03a4a9aa-3964-42f2-853a-5be0c4f5a6a0'
SECRET_KEY = 'e1f0f1a0547b4f43d0f1fbd5bc2d805c4222ce8e'
BUCKET_NAME = 'ce422image'

logging.basicConfig(level=logging.INFO)


def get_s3_client():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )

        return s3_client
    except Exception as exc:
        logging.info(exc)


def get_s3_resource():
    try:
        s3_resource = boto3.resource(
            's3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )

        return s3_resource
    except Exception as exc:
        logging.info(exc)


def bucket_exist(s3_client):
    try:
        response = s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError as err:
        status = err.response["ResponseMetadata"]["HTTPStatusCode"]
        errcode = err.response["Error"]["Code"]

        if status == 404:
            logging.warning("Missing object, %s", errcode)
        elif status == 403:
            logging.error("Access denied, %s", errcode)
        else:
            logging.exception("Error in request, %s", errcode)
    else:
        print(response)


def list_buckets(s3_resource):
    try:
        for bucket in s3_resource.buckets.all():
            logging.info(f'bucket_name: {bucket.name}')
    except ClientError as exc:
        logging.error(exc)


def get_bucket_policy(s3_resource):
    try:
        bucket_policy = s3_resource.BucketPolicy(BUCKET_NAME)
        bucket_policy.load()
        logging.info(bucket_policy.policy)
    except ClientError as e:
        logging.error(e)


def set_bucket_policy(s3_resource):
    try:
        bucket_policy = s3_resource.BucketPolicy(BUCKET_NAME)

        policy = {
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': 'PolicyName',
                'Effect': 'Allow',
                'Principal': '*',
                'Action': ['s3:GetObject'],
                'Resource': f'arn:aws:s3:::{BUCKET_NAME}/user_*'
            }]
        }

        # Convert the policy from JSON dict to string
        policy = json.dumps(policy)
        bucket_policy.put(Policy=policy)
        logging.info(bucket_policy.policy)

    except ClientError as e:
        logging.error(e)


def remove_bucket_policy(s3_resource):
    try:
        bucket_policy = s3_resource.BucketPolicy(BUCKET_NAME)
        bucket_policy.delete()
        logging.info(bucket_policy.policy)
    except ClientError as e:
        logging.error(e)


def get_bucket_accessibility(s3_resource):
    try:
        bucket_acl = s3_resource.BucketAcl(BUCKET_NAME)
        logging.info(bucket_acl.grants)

    except ClientError as e:
        logging.error(e)


def set_bucket_accessibility(s3_resource):
    try:
        bucket_acl = s3_resource.BucketAcl(BUCKET_NAME)
        bucket_acl.put(ACL='private')  # ACL='private'|'public-read'|'public-read-write'

    except ClientError as e:
        logging.error(e)


def get_bucket_tagging(s3_client):
    try:
        response = s3_client.get_bucket_tagging(Bucket=BUCKET_NAME)
        logging.info(response)
    except ClientError as e:
        logging.error(e)


def set_bucket_tagging(s3_client, tag_set):
    try:
        response = s3_client.put_bucket_tagging(
            Bucket=BUCKET_NAME,
            Tagging={
                'TagSet': tag_set
            },
        )
        logging.info(response)
    except ClientError as e:
        logging.error(e)


def remove_bucket_tagging(s3_client):
    try:
        response = s3_client.delete_bucket_tagging(
            Bucket=BUCKET_NAME,
            # ExpectedBucketOwner='string'
        )
        logging.info(response)
    except ClientError as exc:
        logging.error(exc)


"""
S3 upload and download
"""

DOWNLOAD_PATH = '/Users/Bardia/Downloads/img'


def s3_upload(s3_resource, file_path, object_name):
    try:
        bucket = s3_resource.Bucket(BUCKET_NAME)

        with open(file_path, "rb") as file:
            bucket.put_object(
                ACL='private',
                Body=file,
                Key=object_name
            )
    except ClientError as e:
        logging.error(e)


def s3_download(s3_resource, object_name, download_path=DOWNLOAD_PATH):
    try:
        # bucket
        bucket = s3_resource.Bucket(BUCKET_NAME)

        bucket.download_file(
            object_name,
            download_path + '/' + object_name
        )
    except ClientError as e:
        logging.error(e)


def s3_delete(s3_resource, object_name):
    try:
        # bucket
        bucket = s3_resource.Bucket(BUCKET_NAME)
        obj = bucket.Object(object_name)

        response = obj.delete(
            VersionId='string',
        )

        print(response)
    except ClientError as e:
        logging.error(e)


def s3_ls(s3_resource):
    try:
        bucket = s3_resource.Bucket(BUCKET_NAME)

        for obj in bucket.objects.all():
            logging.info(f"object_name: {obj.key}, last_modified: {obj.last_modified}")

    except ClientError as e:
        logging.error(e)


def set_object_tagging(s3_client, object_name, tag_set):
    try:
        response = s3_client.put_object_tagging(
            Bucket=BUCKET_NAME,
            Key=object_name,
            VersionId='string',
            ContentMD5='string',
            Tagging={
                'TagSet': tag_set
            },
            ExpectedBucketOwner='string',
            RequestPayer='requester'
        )
        logging.info(response)
    except ClientError as exc:
        logging.error(exc)


def get_object_tagging(s3_client, object_name):
    try:
        response = s3_client.get_object_tagging(
            Bucket=BUCKET_NAME,
            Key=object_name,
            VersionId='string',
            ExpectedBucketOwner='string',
            RequestPayer='requester'
        )
        logging.info(response)
    except ClientError as exc:
        logging.error(exc)


def remove_object_tagging(s3_client, object_name):
    try:
        response = s3_client.delete_object_tagging(
            Bucket=BUCKET_NAME,
            Key=object_name,
            VersionId='string',
            ExpectedBucketOwner='string'
        )
        logging.info(response)
    except ClientError as exc:
        logging.error(exc)