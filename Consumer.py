#!/usr/bin/python3
import boto3
import time
import json
import decimal
 
# Kinesis setup
kinesis = boto3.client("kinesis", region_name="us-east-1")
shard_id = "shardId-000000000000"
pre_shard_it = kinesis.get_shard_iterator(
    StreamName='txnsjson',
    ShardId=shard_id,
    ShardIteratorType='LATEST'
)
shard_it = pre_shard_it["ShardIterator"]
 
# DynamoDB setup
dynamodb = boto3.resource('dynamodb', region_name="us-east-1")
table = dynamodb.Table('transactions')
 
while 1==1:
        out = kinesis.get_records(ShardIterator=shard_it, Limit=100)
        for record in out['Records']:
                print(record)
                data = json.loads(record['Data'])
                txnid = int(data['txnid'])
                txndate = data['txndate']
                custid = int(data['custid'])
                amount = data['amount']
                category = data['category']
                subcategory = data['subcategory']
                city = data['city']
                state = data['state']
                txntype = data['txntype']
 
                # Construct a unique sort key for this line item
                #orderID = invoice + "-" + stockCode
 
                response = table.put_item(
                    Item = {
                            'txnid': decimal.Decimal(customer),
                            'txndate': txndate,
                            'custid': decimal.Decimal(custid),
                            'amount': amount,
                            'category': category,
                            'subcategory': subcategory,
                            'city': city,
                            'state':state,
                            'txntype':txntype
                            }
                    )
 
        shard_it = out["NextShardIterator"]
        time.sleep(1.0)
