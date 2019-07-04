'use strict';

const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const processResponse = require('./process-response');
const TABLE_NAME = process.env.TABLE_NAME;
const IS_CORS = true;
const PRIMARY_KEY = process.env.PRIMARY_KEY;
const uuidv4 = require('uuid/v4');

exports.handler = (event) => {
    console.log("event: " + JSON.stringify(event));
    
    let params = {
        TableName: TABLE_NAME
    }
    
    if (event.httpMethod === 'OPTIONS') {
		return Promise.resolve(processResponse(IS_CORS));
	}
	
	if (event.httpMethod === 'GET') {
		return dynamoDb.scan(params)
        .promise()
        .then(response => (processResponse(IS_CORS, response.Items)))
        .catch(err => {
            console.log(err);
            return processResponse(IS_CORS, err, 500);
        });
	}
	
	if (event.httpMethod === 'PUT') {
	    let item = {};
	    item.payload = JSON.parse(event.body);
	    item.time = new Date().getTime();
	    item[PRIMARY_KEY] = uuidv4();
        item.thing = event.pathParameters.proxy.split('/')[0];
        let params = {
            TableName: TABLE_NAME,
            Item: item
        }
        return dynamoDb.put(params)
        .promise()
        .then(data => (processResponse(IS_CORS, item[PRIMARY_KEY])))
        .catch(dbError => {
            let errorResponse = `Error: Execution update, caused a Dynamodb error, please look at your logs.`;
            if (dbError.code === 'ValidationException') {
                if (dbError.message.includes('reserved keyword')) errorResponse = `Error: You're using AWS reserved keywords as attributes`;
            }
            console.log(dbError);
            return processResponse(IS_CORS, errorResponse, 500);
        });
	}
    
    return processResponse(IS_CORS, event);
};