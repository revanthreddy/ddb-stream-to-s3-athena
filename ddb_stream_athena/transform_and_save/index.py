import gzip
import shutil
import logging
import os
import boto3
import glob
from boto3.dynamodb.types import TypeDeserializer
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')

output_file = "output.tar.gz"

STORM_EVENT_FATALITIES = "fatalities"
STORM_EVENT_DETAILS = "details"
STORM_EVENT_LOCATIONS = "locations"
STORM_EVENT_OTHERS = "others"


def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v)
        for k, v in dynamo_obj.items()
    }


def function_to_identify_schema(ddb_obj: dict):
    if STORM_EVENT_DETAILS in ddb_obj:
        return STORM_EVENT_DETAILS
    elif STORM_EVENT_LOCATIONS in ddb_obj:
        return STORM_EVENT_LOCATIONS
    elif STORM_EVENT_FATALITIES in ddb_obj:
        return STORM_EVENT_FATALITIES
    else:
        return STORM_EVENT_OTHERS

def handler(event, context):
    """
    :param event:
    {
        'Records': [{
            'eventID': 'b8707f757d728b209879b4cf24091f29',
            'eventName': 'INSERT',
            'eventVersion': '1.1',
            'eventSource': 'aws:dynamodb',
            'awsRegion': 'us-east-1',
            'dynamodb': {
                'ApproximateCreationDateTime': 1679249737.0,
                'Keys': {
                    'id': {
                        'S': '1234'
                    }
                },
                'NewImage': {
                    'hello': {
                        'S': 'world'
                    },
                    'id': {
                        'S': '1234'
                    }
                },
                'SequenceNumber': '3338700000000003613474165',
                'SizeBytes': 22,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            },
            'eventSourceARN': 'arn:aws:dynamodb:us-east-1:154414928612:table/tb_sample_data/stream/2023-03-18T23:04:01.187'
        }]
    }
    :param context:
    :return:
    """
    try:
        bucket_name = os.environ["raw_source_bucket"]

        for record in event['Records']:
            # Extract the new image from the record
            new_image = record['dynamodb']['NewImage']
            new_obj_dict = dynamo_obj_to_python_obj(new_image)


            prefix = function_to_identify_schema(new_obj_dict)
            s3.put_object(
                Body=json.dumps(new_obj_dict),
                Bucket=bucket_name,
                Key=f"formatted/{prefix}/{new_obj_dict['id']}.json"
            )
        return {
            "STATUS": "SUCCESS",
            "MESSAGE": "Files Upload"
        }

    except Exception as e:
        logger.error(e)
        return {
            "STATUS": "FAIL",
            "MESSAGE": "Failed to Upload"
        }

