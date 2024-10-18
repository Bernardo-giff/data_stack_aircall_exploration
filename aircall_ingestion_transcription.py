# Databricks notebook source
# MAGIC %md
# MAGIC # Aircall calls monthly extraction and transcription
# MAGIC
# MAGIC **NOTE: THE LINKS TO THE RECORDINGS EXPIRE:**
# MAGIC
# MAGIC Strategy --> 
# MAGIC
# MAGIC 1. Get all calls aways at a monthly rate (instead of incrementally)
# MAGIC 2. From all calls determine which ones to transcribe / translate
# MAGIC 3. Transcribe / translate only selected calls and store
# MAGIC 4. "Ask" for "summarization" on demmand

# COMMAND ----------

# %pip install openai==0.27.0
%pip install openai --upgrade
# %pip install typing-extensions==4.5.
dbutils.library.restartPython()

# COMMAND ----------

# Getting credentials from Databricks secrets
api_key = dbutils.secrets.get(scope="aircall_project", key="API_KEY")
api_id = dbutils.secrets.get(scope="aircall_project", key="API_ID")
api_key_oai = dbutils.secrets.get(scope="aircall_project", key="API_KEY_OAI")

# Setting up the download_path (i.e: mp3 recordings)
download_path = '/Volumes/dev/aircall_raw/aircall_recordings'

# Transcriptions path
transcription_path = '/Volumes/dev/aircall_raw/call_transcriptions'

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import time
from datetime import datetime, timedelta
import pandas as pd

# Define function to get unix time n days ago
def get_unix_time_n_days_ago(n):
    # Get the current time and subtract 50 days
    time_50_days_ago = datetime.now() - timedelta(days=n)
    # Convert to Unix timestamp
    unix_time = int(time.mktime(time_50_days_ago.timetuple()))
    return unix_time

# Getting 45 days from today
# I am using 45 days here because I believe the frequency will be monthly. So I am taking a period a little over one month to be sure
# days_delta = 45
# While I am testing and running this almost every day, I can use a much smaller window
days_delta = 15

# Get the unix time for 60 days ago
start_date = get_unix_time_n_days_ago(days_delta)

# URL of the API endpoint
url = 'https://api.aircall.io/v1/calls'
params = {
    'order': 'desc',
    'fetch_contact': 'true',
    'per_page': 50,
    'from': start_date
}

# Initialize empty list
all_data = []

# Make the request for the first page
response = requests.get(url, auth=HTTPBasicAuth(api_id, api_key), params=params)
all_data.append(response.json()['calls'])
next_link = response.json()['meta']['next_page_link']

# Iterate through all pages to get the data
while next_link is not None:
    response = requests.get(next_link, auth=HTTPBasicAuth(api_id, api_key))
    all_data.append(response.json()['calls'])
    next_link = response.json()['meta']['next_page_link']

# COMMAND ----------

# MAGIC %md
# MAGIC After saving all of the responses (through pagination) in a json like object, we extract the fields we will retain for the meta_data table. Note that all of this info is readily available in Aircall either from the API or their own data product

# COMMAND ----------

# Extract the required fields
extracted_data = []

for page in all_data:
    for call in page:
        # We need to record the user meta data as null if no one picked up an incoming call
        if call['user'] != None:
            extracted_item = {
                # Defining which fields we will keep
                'id': call['id']
                , 'number_id': call['number']['id']
                , 'number_name': call['number']['name']
                , 'started_at': call['started_at']
                , 'answered_at': call['answered_at']
                , 'ended_at': call['ended_at']
                , 'direction': call['direction']
                , 'recording': call['recording']
                , 'user_id': call['user']['id']
                , 'user_name': call['user']['name']
                , 'user_email': call['user']['email']
                , 'user_available': call['user']['available']
            }
        else:
            extracted_item = {
                # Defining which fields we will keep
                'id': call['id']
                , 'number_id': call['number']['id']
                , 'number_name': call['number']['name']
                , 'started_at': call['started_at']
                , 'answered_at': call['answered_at']
                , 'ended_at': call['ended_at']
                , 'direction': call['direction']
                , 'recording': call['recording']
                , 'user_id': 'null'
                , 'user_name': 'null'
                , 'user_email': 'null'
                , 'user_available': 'null'
            }
        extracted_data.append(extracted_item)

# Create a DataFrame
df = pd.DataFrame(extracted_data)

# Removing not recorded calls
recorded_calls = df[df['recording'].notnull()]

# Changing dates from unix to timestamps
recorded_calls['started_at_ts'] = pd.to_datetime(recorded_calls.loc[:,'started_at'], unit='s')
recorded_calls['answered_at_ts'] = pd.to_datetime(recorded_calls.loc[:,'answered_at'], unit='s')
recorded_calls['ended_at_ts'] = pd.to_datetime(recorded_calls.loc[:,'ended_at'], unit='s')

print(f"There are {len(recorded_calls)} recorded calls")

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we get the calls that are not downloaded yet (by using the call ID from the downloads dir)

# COMMAND ----------

# Define catalog to save the table to
db = 'dev'

# Define schema to save the table to
schema_name = 'aircall_raw'

# Convert the pandas DataFrame to a PySpark DataFrame using the specified schema
spark_df = spark.createDataFrame(recorded_calls)

# Define parameters
database_name = db
schema_name = schema_name
table_name = "recorded_calls"
temp_table = 'temp_' + table_name
# Create temporary table
spark_df.createOrReplaceTempView(temp_table)

# Execute SQL command to create a permanent table in the specified catalog and schema
sql_command = f"""
CREATE OR REPLACE TABLE {database_name}.{schema_name}.{table_name}
USING DELTA
AS SELECT * FROM {temp_table}
"""
spark.sql(sql_command)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev.aircall_raw.call_meta_data

# COMMAND ----------

# Importing the os library to interact with the volume
import os

# Get historical meta data
call_meta_data = _sqldf.toPandas()

# Get all calls already downloaded
calls_downlaoded_files = os.listdir(download_path)

# Extract ids
calls_downlaoded_ids = [call.replace('.mp3','') for call in calls_downlaoded_files]

# Initiate empty lists to store meta data
new_call_id = []
new_call_number_id = []
new_call_number_name = []
new_call_user_id = []
new_call_user_name = []
new_call_user_email = []
new_call_started_at = []
new_call_answered_at = []
new_call_ended_at = []
new_call_direction = []
new_call_user_available = []

# Get calls to donwload
for i, call in recorded_calls.iterrows():
    if not str(call['id']) in calls_downlaoded_ids:
        new_call_id.append(call['id'])
        new_call_number_id.append(call['number_id'])
        new_call_number_name.append(call['number_name'])
        new_call_user_id.append(call['user_id'])
        new_call_user_name.append(call['user_name'])
        new_call_user_email.append(call['user_email'])
        new_call_started_at.append(call['started_at_ts'])
        new_call_answered_at.append(call['answered_at_ts'])
        new_call_ended_at.append(call['ended_at_ts'])
        new_call_direction.append(call['direction'])
        new_call_user_available.append(call['user_available'])

print(f"There are {len(new_call_id)} calls to be downloaded")

# Creating new data to be appended to the current historical data
new_data = {
    'id':new_call_id
    , 'number_id':new_call_number_id
    , 'number_name':new_call_number_name
    , 'user_id': new_call_user_id
    , 'user_full_name': new_call_user_name
    , 'user_email': new_call_user_email
    , 'started_at': new_call_started_at
    , 'answered_at': new_call_answered_at
    , 'ended_at': new_call_ended_at
    , 'direction': new_call_direction
    , 'available': new_call_user_available
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting calls that will be transcribed / translated
# MAGIC
# MAGIC The calls that need to be added are therefore the ones that don't currently exist in the bucket.
# MAGIC
# MAGIC We create a function that download the file to a directory given a call id

# COMMAND ----------

def download_call(id, download_path): 
    """
    This function takes a download directory / path and an Aircall call id and downloads that call into the directory
    """
    # URL of the API endpoint
    url = 'https://api.aircall.io/v1/calls/' + str(id)

    # Get the API response
    response = requests.get(url, auth=HTTPBasicAuth(api_id, api_key), params=params)

    # Get the URL of the call
    recording_url = response.json()['call']['recording']

    # Construct the filename given the directory and the id
    filename = os.path.join(download_path, f"{id}.mp3")

    # Send a GET request to the URL
    response = requests.get(recording_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Open a local file with write-binary mode
        with open(filename, 'wb') as file:
            # Write the content of the response to the file
            file.write(response.content)
    else:
        print('download failed')


# COMMAND ----------

# MAGIC %md
# MAGIC Next, we iterate through all of the new calls (i.e: call id's not present in the current directory) and donwload them. This path is stored in the `recording__dr` variable.

# COMMAND ----------

# Loop through all calls and download the recordings
for call in new_call_id:
    download_call(call, download_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transcribe and translate
# MAGIC
# MAGIC Setting up the OpenAI api's connection, we use the api_key from the OpenAi's api instead of the local setting. This has shown better results in testing.

# COMMAND ----------

# Importing the OpenAI library
from openai import OpenAI

# Initiating the OpenAI client
open_ai_client = OpenAI(
    api_key = api_key_oai
)

# COMMAND ----------

# Empty list of languages of the call to store in the meta_data
languages = []

for id in new_call_id:
    # Settting up file names and directories
    file_mp3 = f"{id}.mp3"
    file_txt = f"{id}.txt"
    file_path_recording = os.path.join(download_path, file_mp3)
    file_path_transcription = os.path.join(transcription_path, file_txt)
    
    # Ensure the file exists and print its size for verification
    try:
        # Open the MP3 file in binary mode
        with open(file_path_recording, "rb") as file_mp3_rb:
            # Create the transcription using the file object
            transcript = open_ai_client.audio.transcriptions.create(
                file=file_mp3_rb,  # Pass the file object here
                model="whisper-1",
                response_format="verbose_json", 
                temperature=0
            )
        
        # Save the transcript text to a .txt file
        with open(file_path_transcription, 'w') as file_txt:
            file_txt.write(transcript.text)  # Correctly write the text content
        
        # Append the detected language to the list
        languages.append(transcript.language)
    
    except Exception as e:
        print(f"An error occurred with file {file_mp3}: {e}")

        # Append null to the language
        languages.append('null')

# Adding the list of languages to the dictionary
new_data['language'] = languages

# Creating new data dataframe
new_data_df = pd.DataFrame(new_data)

# # Appending it to the historical dataframe.
call_meta_data = pd.concat([call_meta_data, new_data_df], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Last step is to save the information back to the schema

# COMMAND ----------

# Convert the pandas DataFrame to a PySpark DataFrame using the specified schema
spark_df = spark.createDataFrame(call_meta_data)

db = 'dev'
schema_name = 'aircall_raw'

# Define parameters
database_name = db
schema_name = schema_name
table_name = "call_meta_data"
temp_table = 'temp_' + table_name
# Create temporary table
spark_df.createOrReplaceTempView(temp_table)

# Execute SQL command to create a permanent table in the specified catalog and schema
sql_command = f"""
CREATE OR REPLACE TABLE {database_name}.{schema_name}.{table_name}
USING DELTA
AS SELECT * FROM {temp_table}
"""
spark.sql(sql_command)
