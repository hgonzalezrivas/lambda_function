import json
import datetime
import time
import os
import uuid
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def lambda_handler(event, context):
    
    logger.info("Event: " + json.dumps(event))
    
    if event['httpMethod'] == "GET":
        
        idGateway = event['pathParameters']['gateway']
        
        try:
            
            table = dynamodb.Table('iot_scanner')
            
            response = table.scan(
                FilterExpression=Attr('idGateway').eq(idGateway) & Attr('eventTime').eq(int(event['queryStringParameters']['last_update']))
            )
            
            items = list()
            
            for i in response['Items']:
                logger.info(i)
                i['eventTime'] = int(i['eventTime'])
                for tracking in i['tracking']:
                    for key, value in tracking['info'].items():
                        if isinstance(value, decimal.Decimal):
                            tracking['info'][key] = float(value)
                items.append(i)
            
            statusCode = 200
            mensaje = items
            
        except Exception as e:
            
            logger.info("Error: " + str(e))
            statusCode = 500
            mensaje = "Bad request."
    
    if event['httpMethod'] == "POST":
        
        try:
            params = json.loads(event['body']) if not isinstance(event['body'], dict) else event['body']
            table = dynamodb.Table('iot_scanner')
            logger.info("Params: " + json.dumps(event['body']))
        
            trackingLst = list()
        
            for info in params['beaconScan']:
                trackingLst.append(info)
        
            dbItem = { 
                "_id": uuid.uuid4().hex,
                "idGateway": event['pathParameters']['gateway'],
                "eventTime": params['eventTime'],
                "tracking": trackingLst
            }
        
            table.put_item(
                Item = dbItem
            )
            
            statusCode = 200
            mensaje = "Success."
            
        except Exception as e:
            
            logger.info("Error: " + str(e))
            statusCode = 500
            mensaje = "Bad request."
            
            
    content = {
        "requestDate": datetime.datetime.utcnow().strftime('%m/%d/%Y %H:%M:%S'),
        "mensaje": mensaje
    }
    
    return {
        "isBase64Encoded": False,
        "statusCode": statusCode,
        "body": json.dumps(content),
        "headers": {
            'Content-Type': 'application/json',
            'charset': 'utf8',
            'Access-Control-Allow-Origin': '*'
        }
    }