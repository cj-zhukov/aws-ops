import boto3
import json
from boto3.dynamodb.types import TypeDeserializer

region = "eu-central-1"
dynamodb = boto3.client('dynamodb', region)
table_name = "foo"
deserializer = TypeDeserializer()
paginator = dynamodb.get_paginator('scan')


def normalize_value(value):
    if isinstance(value, set):
        return list(value)
    elif isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [normalize_value(v) for v in value]
    else:
        return value
    

def decode_item(item):
    deserialized = {k: deserializer.deserialize(v) for k, v in item.items()}
    return normalize_value(deserialized)


all_items = []
for page in paginator.paginate(TableName=table_name):
    for item in page['Items']:
        decoded = decode_item(item)
        all_items.append(decoded)

with open('export2.json', 'w') as f:
    json.dump(all_items, f, indent=2)