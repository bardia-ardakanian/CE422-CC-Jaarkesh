from bson import ObjectId
from Jaarkesh.settings import DEBUG
from app.models import State
from pymongo import MongoClient
from botocore.exceptions import ClientError
import boto3
import pika
import logging
import json
import os.path
import secrets

CONNECTION_STRING = 'mongodb+srv://mongoadmin:Cij3mPHBEvsWr5Jw' \
                    '@cluster0.dpliugt.mongodb.net/?retryWrites=true&w=majority'
DB_NAME = 'Jaarkesh'
PROMOTION_COL = 'Promotion'
IMAGE_COL = 'Image'

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
    return db[PROMOTION_COL]


def get_image_col(db):
    return db[IMAGE_COL]


def get_collection(col_name):
    db = get_db_client()

    col = None
    if col_name == 'Promotion':
        col = get_promotion_col(db)
    if col_name == 'Image':
        col = get_image_col(db)

    return col


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
Promotion State Filter
"""


def get_promotion_by_id(_pid):
    col = get_collection('Promotion')

    return find(col, {"_id": ObjectId(_pid)})[0]


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
if DEBUG:
    UPLOAD_PATH = '/Users/Bardia/Documents/aut/courses/CE422-CC/assignments/jaarkesh-repo/upload'
    DOWNLOAD_PATH = '/Users/Bardia/Documents/aut/courses/CE422-CC/assignments/jaarkesh-repo/download'
else:
    UPLOAD_PATH = '/home/ubuntu/tmp/upload'
    DOWNLOAD_PATH = '/home/ubuntu/tmp/download'


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


def submit(description, email, file_path):
    """
    :param description: promotion description
    :param email: user email
    :param file_path: path to file
    :return:
    """
    # Submit promotion
    promotion_col = get_collection('Promotion')
    _pid = insert(promotion_col, {
        'description': description,
        'email': email,
        'state': State.PROCESSING.value,
        'category': None
    })
    logging.info('Promotion inserted [{0}]'.format(_pid.inserted_id))
    # Upload image
    object_name = generate_name()
    s3_upload(get_s3_resource(), UPLOAD_PATH + file_path, object_name)
    # Map Image_name to Promotion_id
    image_col = get_collection('Image')
    _mid = insert(image_col, {
        'image_name': object_name,
        'promotion_id': _pid.inserted_id
    })
    logging.info('ImageMap inserted [{0}]'.format(_mid.inserted_id))
    # Publish promotion id
    # Must be string because ObjectId datatype does not have len property which causes RabbitMQ to crash
    mq_publish_promotion_id(str(_pid.inserted_id))

    return _pid.inserted_id, _mid.inserted_id


def generate_name():
    return secrets.token_hex(16) + '.jpeg'


"""
RabbitMQ Message broker service
"""

AMQP_URL = 'amqps://visptmhc:LN7y3ZvEWB4t8HTIrXgPuTb3gmuwWW0-@stingray.rmq.cloudamqp.com/visptmhc'
QUEUE = 'PROMOTION_QUEUE'


def mq_make_connection():
    return pika.BlockingConnection(pika.URLParameters(AMQP_URL))


def mq_close_connection(connection):
    connection.close()


def mq_get_channel(connection):
    return connection.channel()


def mq_make_queue(channel, queue):
    channel.queue_declare(queue=queue)


def mq_publish(channel, routing, body):
    channel.basic_publish(exchange='', routing_key=routing, body=body)
    logging.info(" [x] Sent {0}".format(body))


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


def mq_consume(channel):
    channel.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def mq_publish_promotion_id(_pid):
    conn = mq_make_connection()
    channel = mq_get_channel(conn)

    mq_publish(channel, QUEUE, _pid)

    mq_close_connection(conn)


# print(State(1))
# promotion_col = get_collection('Promotion')
# x = find(promotion_col, {"_id": ObjectId('63769aa39b4865d2ced554ad')})
# for i in x:
#     print(x)
# image_col = get_collection('Image')
# delete_all(promotion_col)
# delete_all(image_col)
#
# promotions = [
#     {
#         'description': 'Hi, Mom!',
#         'email': '1@email.com',
#         'state': State.PROCESSING.value,
#         'category': None
#     },
#     {
#         'description': 'Hi, Dad!',
#         'email': '2@email.com',
#         'state': State.ACCEPTED.value,
#         'category': None
#     },
#     {
#         'description': 'Hi, Bro!',
#         'email': '3@email.com',
#         'state': State.REJECTED.value,
#         'category': None
#     },
# ]
#
# insert_many(promotion_col, promotions)
# image_col = get_collection('Image')
# delete_all(promotion_col)
# delete_all(image_col)
#
# pid, mid = submit_promotion(
#     "Test01. John Doe",
#     "JohnDoe@email.com",
#     UPLOAD_PATH + "/monkey.jpg",
#     secrets.token_hex(16) + '.jpeg'
# )
#
# image_map = find(image_col, {"_id": mid})[0]
# print(image_map)
#
# s3_download(get_s3_resource(), image_map["image_name"])
#
# conn = mq_make_connection()
# channel = mq_get_channel(conn)
#
# mq_consume(channel)
#
# mq_close_connection(conn)
