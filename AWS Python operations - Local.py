#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
import boto3
from numpy import nan
import json
pd.set_option('display.max_columns', None)


# In[3]:


# get access credentials from JSON file
aws_access_cred_df = pd.read_csv('C:/Users/*.csv')
access_key = aws_access_cred_df.iloc[0]['Access key ID']
secret_key = aws_access_cred_df.iloc[0]['Secret access key']


# Retrieve secret from Secrets Manager

# In[15]:


secret_name = '*'

#Secret Manager is used to store various credentials: both for AWS services (like RDS database)
# and third party software (like Salesforce and Snowflake)

#create connection to secretsmanager
secret_manager = boto3.client('secretsmanager',aws_access_key_id=access_key, aws_secret_access_key=secret_key,  region_name="eu-central-1")
# get json with secret
get_secret_value_response = json.loads(secret_manager.get_secret_value(SecretId=secret_name)['SecretString'])

#close conneciton
secret_manager.close()

# convert JSON response to variables
user=get_secret_value_response['username']
pas=get_secret_value_response['password']


# S3 Operations

# In[12]:


bucket_name = '*'
aws_folder_name = '*'
aws_file_name = '*.csv'

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

#Read csv file into pandas dataframe
# Use the `get_object` method to get the contents of the file
response = s3.get_object(Bucket=bucket_name, Key=f'{aws_folder_name}/{aws_file_name}')

# Read CSV data into a Pandas DataFrame
df = pd.read_csv(response['Body'], sep='\t')
df.tail(1)


# In[ ]:


#upload existing file to S3
file_path=aws_file_name #file name/path of local file (change if you want to rename the file)
s3_key = f'{aws_folder_name}/{aws_file_name}'  # The object key in S3 - destination filepath/name on S3

# Upload the file to S3
s3.upload_file(file_path, bucket_name, s3_key)


# In[ ]:


#save dataframe to S3 directly

from io import StringIO

#convert dataframe to string format
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

#save dataframe
s3.Object(bucket_name, s3_key).put(Body=csv_buffer.getvalue())

# close session
s3.close()


# RDS

# In[17]:


from sqlalchemy import create_engine
from sqlalchemy import text

# RDS PostgreSQL database configuration details
db_name = "*"
db_endpoint = "*.eu-central-1.rds.amazonaws.com" #could be diferent depending where script is being run from

# SQLAlchemy database connection string
database_uri = f"postgresql://{user}:{pas}@{db_endpoint}/{db_name}"
engine = create_engine(database_uri).connect()

#GET DATA FROM SERVER
df_rds = pd.read_sql_query('''
select * from public.* LIMIT 100                         
''', con = engine)

df_rds.tail(2)


# In[ ]:


# Upload dataframe to table
df_rds.to_sql('*', schema='public', con=engine, index=False, if_exists='append') # 'replace', 'fail'

#close conneciton
engine.close()

