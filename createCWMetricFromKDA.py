from __future__ import print_function
import boto3
import base64
from json import loads

cloudwatch_client = boto3.client('cloudwatch')

# this function consumes KDA output stream and sends custom metric to CloudWatch
# sample KDA output record
# {"ROWTIME_TIMESTAMP":"2019-10-24 08:03:24.024","UNIQUE_USER_COUNT":"1"}

def lambda_handler(event, context):
    payload = event['records']
    output = []
    success = 0
    failure = 0

    for record in payload:
        try:
            # Parse a record          
            payload = base64.b64decode(record['data'])
            data_item = loads(payload)

            # Submit CloudWatch custom metric data
            response = cloudwatch_client.put_metric_data(
                MetricData = [
                    {
                        'MetricName': 'CCU',
                        'Dimensions': [
                            {
                                'Name': 'GAME',
                                'Value': 'Stupid Clicker'
                            }
                        ],
                        'Unit': 'Count',
                        'Value': data_item['UNIQUE_USER_COUNT']
                    }
                ],
                Namespace='Clicker'
            )

            success += 1
            output.append({'recordId': record['recordId'], 'result': 'Ok'})
        except Exception as error:
            failure += 1
            output.append({'recordId': record['recordId'], 'result': 'DeliveryFailed'})
            print("error is", error)

    print('Successfully delivered {0} records, failed to deliver {1} records'.format(success, failure))
    return {'records': output}