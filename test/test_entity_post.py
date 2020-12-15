import json
import time

import boto3
import base64

import requests
from requests.exceptions import Timeout

import keyring

 
MAX_BATCH_ROWS=25
FACTSET_API_READ_TIMEOUT=25

# 200 is the HTTP status code for "ok".
status_code = 200

# The return value will contain an array of arrays (one inner array per input row).
array_of_rows_to_return = [ ]

col_names=['name','country','state','url']

event={"body": { "data": [ [ 0 ,"Tesla Inc.","US",None,"www.tesla.com"],[ 1 ,"Snowflake","US",None,"www.snowflake.com"],[ 2 ,"AT&T","US",None,"www.att.com"]] }}
event_body = event["body"]
rows = event_body["data"]



# initialize request  object
headers={'Content-type': 'application/json;charset=UTF-8', 'Accept': 'application/json'}
url='https://api.factset.com/content/factset-concordance/v1/entity-match'

secret={}
secret['APIUser']='snowflakedev-1135273'
secret['APIKey']=keyring.get_password('FACTSET',secret['APIUser'])


session=requests.Session()
session.auth=(secret['APIUser'],secret['APIKey'])
timeout=(FACTSET_API_READ_TIMEOUT)

# initialize the parameter object send to the API. It's a dictionary with an array named input 
# holding dictionaries with the company match information
data={}
data['input']=[]

# For each input row in the JSON object...

for row in rows:
    # Read the input row number (the output row number will be the same).
    row_number = row[0]

    # Read all not null values and add them to the filter

    request = {}
    
    for i in range(1,len(row)):
        if not (row[i] == None):
            request[col_names[i-1]]=row[i]
    
    # append the request to the existing input array            
    data['input'].append(request)
    
    # also copy the company match information into an output object
    array_of_rows_to_return.append([row_number,[request]])
                
response=session.post(url,data=json.dumps(data),headers=headers,timeout=timeout)

print(response)