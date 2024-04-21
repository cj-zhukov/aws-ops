import boto3

bucket = "foo"
prefix = "bar"

client = boto3.client('s3')
s3_paginator = client.get_paginator('list_objects_v2')
s3_page_iterator = s3_paginator.paginate(Bucket=bucket, Prefix=prefix)
keys = []
for s3_page_response in s3_page_iterator:
    for s3_object in s3_page_response['Contents']:
        key = s3_object['Key']
        keys.append(key)
