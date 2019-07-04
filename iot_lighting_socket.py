import re
import logging
import json
import bson
import datetime
import time
import os
from bson.json_util import dumps
import requests
from requests_aws4auth import AWS4Auth
from requests_aws4auth.aws4signingkey import AWS4SigningKey
import uuid
import boto3
from boto3.dynamodb.conditions import Key, Attr
import aws
import decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
lambdaClient = boto3.client('lambda')

def lambda_handler(event, context):
    
    logger.info("Event: " + dumps(event))
    logger.info("Request Context: " + dumps(event['requestContext']))
    
    requestContext = event['requestContext']
    
    table = dynamodb.Table('iotmx-lighting-socket')
    
    try:
        if requestContext['routeKey'] == "$connect":
            socketDTO = {}
            
            socketDTO['connectionId'] = requestContext['connectionId']
            socketDTO['connectionTime'] = requestContext['connectedAt']
            socketDTO['domainName'] = requestContext['domainName']
            socketDTO['identity'] = requestContext['identity']
            socketDTO['online'] = True
            socketDTO['_id'] = uuid.uuid4().hex
            
            logger.info("Putting item in iotmx-lighting-socket: " + dumps(socketDTO))
            
            table.put_item(Item = socketDTO)
            
            statusCode = 200
            mensaje = "Connected."
            
        elif requestContext['routeKey'] == "message":
            
            statusCode = 200
            
            logger.info("Message: " + dumps(event['body']))
            
            onlineClient = list()
            response = table.scan(
                FilterExpression=Attr('online').eq(True)
            )
            
            for i in response['Items']:
                onlineClient.append(i['connectionId'])
            
            if requestContext['connectionId'] in onlineClient:
                onlineClient.remove(requestContext['connectionId'])
            
            if len(onlineClient) != 0:
                #Post message to connected clients
                date = (datetime.datetime.utcnow()).strftime('%Y%m%d')
                auth = AWS4Auth(aws.access_id, AWS4SigningKey(aws.secret_key, aws.region, 'execute-api', date))
                headers = {
                    'Context-Type': 'application/json',
                    'X-Amz-Date': (datetime.datetime.utcnow()).strftime('%Y%m%dT%H%M%SZ'),
                    'X-Amz-Client-Context': dumps({})
                }
                for id in onlineClient:
                    endpoint = "https://" + requestContext['domainName'] + "/v1/@connections/" + str(id)
                    messageDTO = json.loads(event['body'])
                    payload = messageDTO['data']
                    r = requests.post(url = endpoint, data = dumps(payload), auth = auth, headers = headers)
                    
            params = json.loads(event['body'])
            
            paramsDTO = {
                "httpMethod": "POST",
                "body": params['data']
            }
            
            logger.info("paramsDTO: " + dumps(paramsDTO))
            
            response = lambdaClient.invoke(
                FunctionName = 'iotmx-lighting-device',
                InvocationType = 'Event',
                Payload = dumps(paramsDTO)
            )
            
            logger.info("Lambda Invokation: " + dumps(response))
            
            mensaje = "Message received."                
        
        elif requestContext['routeKey'] == "$disconnect":
            
            client = {}
            response = table.scan(
                FilterExpression=Attr('connectionId').eq(requestContext['connectionId'])
            )
            
            for i in response['Items']:
                client = i
            
            if len(client) != 0:
                
                logger.info("Connection Id: " + dumps(client['connectionId']))
                
                table.delete_item(
                    Key = {
                        "_id": client['_id']
                })
            
            statusCode = 200
            mensaje = "Desconexion de socket"
            
        else:
            
            statusCode = 200
            mensaje = requestContext['routeKey']
        
            
    except Exception as e:
        
        logger.info("Error: " + str(e))
        
        statusCode = 500
        mensaje = "Error: " + str(e)
	
    return {
        "isBase64Encoded": False,
        "statusCode": statusCode,
        "body": mensaje,
        "headers": {
             'Content-Type': 'application/json',
            'charset': 'utf8',
            'Access-Control-Allow-Origin': '*'
        }
    }
