import json
import time
import re

import boto3
import base64

import requests
from requests.exceptions import Timeout


def get_secret():

    secret_name = "FactsetAPICredentials"
    region_name = "us-west-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            
    return secret 

def lambda_handler(event, context):
 
    MAX_BATCH_ROWS=1
    FACTSET_API_READ_TIMEOUT=25

    begin_ts=time.time()
    
    # 200 is the HTTP status code for "ok".
    status_code = 200
    
    # The return value will contain an array of arrays (one inner array per input row).
    array_of_rows_to_return = [ ]
    
    col_names=['ids']
 
    try:
        # From the input parameter named "event", get the body, which contains
        # the input rows.
        event_body = event["body"]
 
        # Convert the input from a JSON string into a JSON object.
        payload = json.loads(event_body)
        # This is basically an array of arrays. The inner array contains the
        # row number, and a value for each parameter passed to the function.
        
        rows = payload["data"]
        row_count=len(rows)
        if (len(rows) > MAX_BATCH_ROWS):
            status_code = 400;
            json_compatible_string_to_return=json.dumps({"data" : ["Too many rows in batch; Set MAX_BATCH_ROWS="+str(MAX_BATCH_ROWS) ] })
        else:
        
            # Get Credentials from Secret Manager
            ssm_begin_ts=time.time()
            secret = json.loads(get_secret());
            ssm_end_ts=time.time()
            
            ssm_response_time_ms=int(((ssm_end_ts-ssm_begin_ts)*1000)/row_count)
            
            # initialize request  object
            session=requests.Session()
            session.headers.update={'Content-type': 'application/json', 'Accept': 'application/json'}
            session.auth=(secret['APIUser'],secret['APIKey'])
            timeout=(FACTSET_API_READ_TIMEOUT)
    
            url='https://api.factset.com/content/symbology/v2/factset'
     
            # For each input row in the JSON object...
        
            for row in rows:
                # Read the input row number (the output row number will be the same).
                row_number = row[0]
     
                # Read the first input parameter's value. For example, this can be a
                # numeric value or a string, or it can be a compound value such as
                # a JSON structure.
                
                # Read all not null values and add them to the 
                output_row = {}

                params={}
                column_count=min(len(col_names)+1,len(row))

                #output_row['ip']=requests.get('https://api.ipify.org').text
                
                for i in range(1,column_count):
                    if not (row[i] == None):
                        value=row[i] #.encode('ascii','ignore').decode()
                        output_row[col_names[i-1]]=value
                        params[col_names[i-1]]=value
                
                try:

                    api_begin_ts=time.time()
                    response=session.get(url,params=params,timeout=timeout)
                    api_end_ts=time.time()
                    
                    
                    output_row['api_response_time_ms']=int((api_end_ts-api_begin_ts)*1000)
                    
                    # Compose the output based on the input. This simple example
                    # merely echoes the input by collecting the values into an array that
                    # will be treated as a single VARIANT value.
                    response.raise_for_status()
                    output_row['status'] = response.status_code
                    try:
                        output_row['response'] = response.json()
                    except Exception as err:
                        pass

                except Timeout as err:
                    output_row['status'] = 408
                    output_row['response'] = str(err)
                    
                except Exception as err:
                    output_row['status'] = 400
                    output_row['response'] = str(err)

                end_ts=time.time()
                output_row['billing_response_time_ms'] = output_row['api_response_time_ms']+ssm_response_time_ms
                output_value = [output_row]
    
                # Put the returned row number and the returned value into an array.
                row_to_return = [row_number, output_value]
     
                # ... and add that array to the main array.
                array_of_rows_to_return.append(row_to_return)
 
            json_compatible_string_to_return = json.dumps({"data" : array_of_rows_to_return})
            
    except Exception as err:
        # 400 implies some type of error.
        status_code = 400
        # Tell caller what this function could not handle.
        json_compatible_string_to_return = str(err) #event_body
    
    # Return the return value and HTTP status code.
    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }
