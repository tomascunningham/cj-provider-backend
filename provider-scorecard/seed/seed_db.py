import boto3
import json
from decimal import Decimal
from copy import deepcopy

tablename = 'provider-scorecard'

ddb = boto3.resource(
    'dynamodb', 
    endpoint_url='http://dynamodb:8000',
    region_name='us-east-1'
)

if __name__ == "__main__":
    try:
        ddb.create_table(
            TableName=tablename,
            AttributeDefinitions=[
                {
                    'AttributeName': 'npi',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'npi',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'year',
                    'KeyType': 'RANGE'
                }
            ],
            ProvisionedThroughput= {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        print('Successfully created Table')

    except Exception as e:
            # cant catch the actual exception when using resource. https://github.com/boto/boto3/issues/1262
            if e.__class__.__name__ == 'ResourceInUseException':
                print('table already exists')
                pass
            else:
                raise e

    table = ddb.Table(tablename)

    test_items = [
        {
            "npi": "t_1234567890",
            "year": 2019,
            "test": True,
            "regions": ['Virginia', 'Maryland'],
        },
        {
            "npi": "t_1234567890",
            "year": 2020,
            "test": True,
            "regions": ['Virginia', 'Maryland'],
        },
    ]

    for item in test_items:
        table.put_item(Item=item)

    keys = [
        {"npi": "1010110010", "year": 2019},
        {"npi": "1010110010", "year": 2020},
        {"npi": "1010110011", "year": 2019},
        {"npi": "1010110011", "year": 2020},
    ]

    with open('sample_item.json') as f:
        template = json.load(f)

        for key in keys:
            data = deepcopy(template)
            data["npi"] = key["npi"]
            data["year"] = key["year"]
            data = json.loads(json.dumps(data), parse_float=Decimal)
            table.put_item(Item=data)

        scanResponse = table.scan(TableName=tablename)
        items = scanResponse['Items']

        if len(items):
            print("Created the following items")

            for item in items:
                print(item["npi"], item["year"])
