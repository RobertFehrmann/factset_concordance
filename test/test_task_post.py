import json
import time
import re
import io
import uuid

import boto3
import base64

import requests
from requests.exceptions import Timeout

import keyring

MAX_BATCH_ROWS=1000
FACTSET_API_READ_TIMEOUT=25

begin_ts=time.time()

# 200 is the HTTP status code for "ok".
status_code = 200

# The return value will contain an array of arrays (one inner array per input row).
array_of_rows_to_return = [ ]

col_names=['name','country','state','url']
form_names=['nameColumn','countryColumn','stateColumn','urlColumn']
event={"body": { "data": [ [ 0 ,"TSLA","Tesla Inc.","US",None,"www.tesla.com"],[ 1 ,"SNOW","Snowflake","US",None,"www.snowflake.com"],[ 2 ,"ATT","AT&T","US",None,"www.att.com"]] }}
event_body = event["body"]
rows = event_body["data"]

payload={}   
files={}
payload['taskName']='Snowflake_' +uuid.uuid4().hex
files['taskName']='Snowflake_' +uuid.uuid4().hex
file=io.StringIO(initial_value='',newline='\n')

# create mapping between filter columns and column names 
        
file.write('row_number')
for i in range(0,len(form_names)):
    file.write(',')
    files[form_names[i]]=col_names[i]
    payload[form_names[i]]=col_names[i]    
    file.write(col_names[i])

headers={'Content-Type': 'multipart/form-data;charset=utf-8', 'Accept': 'application/json;charset=utf-8'}
url='https://api.factset.com/content/factset-concordance/v1/entity-task'


secret={}
secret['APIUser']='snowflakedev-1135273'
secret['APIKey']=keyring.get_password('FACTSET',secret['APIUser'])

session=requests.Session()
session.auth=(secret['APIUser'],secret['APIKey'])
timeout=(FACTSET_API_READ_TIMEOUT)
#session.headers.update={'Content-Type': 'multipart/form-data', 'Accept': 'application/json;charset=utf-8'}

for row in rows:
    file.write('\n')
    row_number = row[0]
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

files={}
# add the encoded content of the file object to the files parameter 
files['inputFile']=(file.getvalue()).encode('utf-8')
#files['inputFile']=('snowflake_test3',open('/Users/rfehrmann/Downloads/snowflake_test3.csv',mode='r',encoding='utf-8'),'text/csv')

response=session.post(url, files=files, data=payload, timeout=timeout)
print(response)