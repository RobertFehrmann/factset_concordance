import json
import time
import re
import io
import uuid

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
 
    MAX_BATCH_ROWS=1000
    FACTSET_API_READ_TIMEOUT=25

    begin_ts=time.time()
    
    # 200 is the HTTP status code for "ok".
    status_code = 200
    
    # The return value will contain an array of arrays (one inner array per input row).
    array_of_rows_to_return = [ ]
    
    col_names=['name','country','state','url']
    form_names=['nameColumn','countryColumn','stateColumn','urlColumn']

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
            status_code = 400
            json_compatible_string_to_return="Too many rows in batch; Set MAX_BATCH_ROWS="+str(MAX_BATCH_ROWS)
        else:
        
            # Get Credentials from Secret Manager
            ssm_begin_ts=time.time()
            secret = json.loads(get_secret())
            ssm_end_ts=time.time()
            
            ssm_response_time_ms=int(((ssm_end_ts-ssm_begin_ts)*1000)/row_count)
            
            # create a files object, unique identification for the uploaded file 
            payload={}
            payload['taskName']='Snowflake_'+uuid.uuid4().hex

            file=io.StringIO(initial_value='',newline='\n')

            # create mapping between filter columns and column names            
            file.write('row_number')
            for i in range(0,len(form_names)):
                file.write(',')
                payload[form_names[i]]=col_names[i]
                file.write(col_names[i])
            
            # initialize request  object
            #headers={'Content-Type': 'multipart/form-data;charset=UTF-8', 'Accept': 'application/json'}
            url='https://api.factset.com/content/factset-concordance/v1/entity-task'
            session=requests.Session()
            session.auth=(secret['APIUser'],secret['APIKey'])
            timeout=(FACTSET_API_READ_TIMEOUT)
            #session.headers.update={'Content-Type': 'multipart/form-data;charset=UTF-8', 'Accept': 'application/json'}
            
            # For each input row in the JSON object...
            for row in rows:
                
                # Read the input row number (the output row number will be the same).
                file.write('\n')
                row_number = row[0]
     
                # map the row to an output object and also write the values into the
                # file object send to the API
                output_row = {}
                column_count=min(len(col_names)+1,len(row))

                file.write(str(row_number))
                for i in range(1,column_count):
                    file.write(',')
                    if not (row[i] == None):
                        output_row[col_names[i-1]]=row[i]
                        file.write('"'+row[i]+'"')
                
                # add an output object to the output array
                array_of_rows_to_return.append([row_number, [output_row]])

            # add the encoded content of the file object to the files parameter 
            files={}
            files['inputFile']=(file.getvalue()).encode('utf-8')
            try:

                api_begin_ts=time.time()
                response=session.post(url, files=files, data=payload, timeout=timeout)
                api_end_ts=time.time()

                end_ts=time.time()
                api_response_time_ms=int((api_end_ts-api_begin_ts)*1000)
                billing_response_time_ms = api_response_time_ms+ssm_response_time_ms
                
                response.raise_for_status()

                # collect debug information
                array_of_rows_to_return[0][1][0]['debug']={}
                array_of_rows_to_return[0][1][0]['debug']['api_response_time_ms']=api_response_time_ms
                array_of_rows_to_return[0][1][0]['debug']['billing_response_time_ms']=billing_response_time_ms
                array_of_rows_to_return[0][1][0]['debug']['file']=file.getvalue()
                array_of_rows_to_return[0][1][0]['debug']['api_response']=(response.json())['data']
                array_of_rows_to_return[0][1][0]['debug']['api_status']=200

                # add task ID and task status to every row                 
                for row_number in range(0,len(array_of_rows_to_return)):
                    # Read the input row number (the output row number will be the same).
                    try:
                        array_of_rows_to_return[row_number][1][0]['taskId']=(response.json())['data']['taskId']
                        array_of_rows_to_return[row_number][1][0]['taskStatus']=(response.json())['data']['status']
                        array_of_rows_to_return[row_number][1][0]['rowIndex']=row_number
                    except:
                        array_of_rows_to_return[row_number][1][0]['error']="taskId not found"

                # return the results objects    
                json_compatible_string_to_return = json.dumps({"data" : array_of_rows_to_return})

            except Timeout as err:
                status_code=408
                json_compatible_string_to_return="HTTP Timeout: "+ url + " exceeded "+str(timeout)+" seconds"
                
            except Exception as err:
                status_code=response.status_code
                json_compatible_string_to_return="Error calling "+ url + ": " + response.text

    except Exception as err:
        # 400 implies some type of error.
        status_code = 400
        # Tell caller what this function could not handle.
        json_compatible_string_to_return = event_body
    
    # Return the return value and HTTP status code.
    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return,
    }
