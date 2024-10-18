# Databricks notebook source
# MAGIC %md
# MAGIC # Call summarization by ICP customer
# MAGIC
# MAGIC The goal of this algorithm is to bundle all calls from the same ICP customer into a single file (or bundle of files) and pass it on to the API with the Prompt provided by the Product team.

# COMMAND ----------

# %pip install openai==0.27.0
%pip install openai --upgrade
# %pip install typing-extensions==4.5.
dbutils.library.restartPython()

# COMMAND ----------

# Importing the OpenAI library
from openai import OpenAI
import os
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

# Getting credentials from Databricks secrets
api_key = dbutils.secrets.get(scope="aircall_project", key="API_KEY")
api_id = dbutils.secrets.get(scope="aircall_project", key="API_ID")
api_key_oai = dbutils.secrets.get(scope="aircall_project", key="API_KEY_OAI")


# Transcriptions path
transcription_path = '/Volumes/dev/aircall_raw/call_transcriptions'

# Summaries path
summaries_path = '/Volumes/dev/aircall_raw/call_summaries'

# Initiating the OpenAI client
open_ai_client = OpenAI(
    api_key = api_key_oai
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Warning**
# MAGIC This call:
# MAGIC /Volumes/dev/aircall_raw/call_transcriptions/2050769882.txt
# MAGIC
# MAGIC If we look at the transcription:
# MAGIC
# MAGIC > Die vier, fünf Paletten, die ich dir anliefere, zeigst du im Kupfermaterial, hast du die Möglichkeit, dass du zwei Euro-Paletten zum Tauschen hast? Ja, das ist kein Problem, haben wir hier. Okay, perfekt, dann mache ich das mit dem Kunden aus, sehr gut, alles klar, passt, dann wünsche ich noch einen schönen Tag und wir hören uns. Dir auch, danke, mach's gut, ciao.
# MAGIC
# MAGIC It basically skips the greetings and everything. This speaks about the quality of the transcriptions. The core message is there, but several things were not recorded. I noticed that the part that was not transcribed has a lot of overlap of conversations. Although one can understand they are greeting each other, for a model that might not be that obvious.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get data that needs to be summarized

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining the ICP accounts
# MAGIC
# MAGIC Next, we will define what constitutes an ICP account. It is not entirely necessary that we have a final version of this now (we can come back later on)

# COMMAND ----------

# MAGIC %md
# MAGIC In this first approach, we are defining all of the accounts created after 2024-01-01 and associated with an inboud opportunity as an ICP account

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH 
# MAGIC   calls AS (
# MAGIC     SELECT
# MAGIC       Id
# MAGIC       , WhoId
# MAGIC       , WhatId
# MAGIC       , AccountId
# MAGIC       , OwnerId
# MAGIC       , Subject
# MAGIC       , aircall__Country__c
# MAGIC       , aircall__Call_Recording__c
# MAGIC       , CallDisposition
# MAGIC       , aircall__Detailed_type__c
# MAGIC       , aircall__Connection_status__c
# MAGIC       , aircall__Answered_by__c
# MAGIC       , CompletedDateTime
# MAGIC       , CallDurationInSeconds
# MAGIC       , CallType
# MAGIC       , CreatedDate
# MAGIC     FROM prd.bronze.salesforce_task
# MAGIC     -- Removing the calls where the recording is not available
# MAGIC     WHERE aircall__Call_Recording__c != 'null'
# MAGIC   )
# MAGIC
# MAGIC   -- Only keep account involved in at least one pro-form opp
# MAGIC   , inbound_accounts AS (
# MAGIC       SELECT distinct fk_account
# MAGIC       FROM prd.gold.fct_opportunity
# MAGIC       WHERE 
# MAGIC         -- Accounts related to opps from the pro-form
# MAGIC         flg_is_from_pro_form = true
# MAGIC   )
# MAGIC   -- Only accounts created this year and related to the pro-form
# MAGIC   , accounts_created_post_icp AS (
# MAGIC       SELECT 
# MAGIC         pk_account
# MAGIC         , ds_account
# MAGIC       FROM prd.silver.stg_salesforce__account
# MAGIC       WHERE 
# MAGIC         ts_created_at >= '2024-07-01'
# MAGIC         AND pk_account IN (SELECT fk_account FROM inbound_accounts)
# MAGIC   )
# MAGIC
# MAGIC   , users AS (
# MAGIC       SELECT * FROM prd.gold.dim_user
# MAGIC   )
# MAGIC
# MAGIC   , final AS (
# MAGIC       SELECT 
# MAGIC         calls.* 
# MAGIC         , users.ds_full_name
# MAGIC         , accounts_created_post_icp.ds_account
# MAGIC       FROM calls
# MAGIC       LEFT JOIN users
# MAGIC       ON calls.OwnerId = users.pk_user
# MAGIC       INNER JOIN accounts_created_post_icp
# MAGIC       ON calls.AccountId = accounts_created_post_icp.pk_account
# MAGIC   )
# MAGIC
# MAGIC SELECT * FROM final
# MAGIC

# COMMAND ----------

import pandas as pd
# Bring pyspark data to Pandas
sf_calls = _sqldf.toPandas()

# Extract the call id after calls/ from the url with regex
sf_calls['call_id'] = sf_calls['aircall__Call_Recording__c'].str.extract(r'calls/(\d+)')

# Sort calls in descending order by account and created date
sf_calls = sf_calls.sort_values(['AccountId', 'CreatedDate'], ascending=True)

sf_calls.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining calls by account

# COMMAND ----------


# Initiating the OpenAI client
open_ai_client = OpenAI(
    api_key = api_key_oai
)

# Define function to download call
def download_call(id, download_path): 
    """
    This function takes a download directory / path and an Aircall call id and downloads that call into the directory
    """
    # URL of the API endpoint
    url = 'https://api.aircall.io/v1/calls/' + str(id)

    # Get the API response
    response = requests.get(url, auth=HTTPBasicAuth(api_id, api_key))

    # # Form new dictionary with call parameters
    # call = response.json()['call']

    # if call['user'] != None:
    #     extracted_item = {
    #         # Defining which fields we will keep
    #         'id': call['id']
    #         , 'number_id': call['number']['id']
    #         , 'number_name': call['number']['name']
    #         , 'started_at': call['started_at']
    #         , 'answered_at': call['answered_at']
    #         , 'ended_at': call['ended_at']
    #         , 'direction': call['direction']
    #         , 'recording': call['recording']
    #         , 'user_id': call['user']['id']
    #         , 'user_name': call['user']['name']
    #         , 'user_email': call['user']['email']
    #         , 'user_available': call['user']['available']
    #     }
    # else:
    #     extracted_item = {
    #         # Defining which fields we will keep
    #         'id': call['id']
    #         , 'number_id': call['number']['id']
    #         , 'number_name': call['number']['name']
    #         , 'started_at': call['started_at']
    #         , 'answered_at': call['answered_at']
    #         , 'ended_at': call['ended_at']
    #         , 'direction': call['direction']
    #         , 'recording': call['recording']
    #         , 'user_id': 'null'
    #         , 'user_name': 'null'
    #         , 'user_email': 'null'
    #         , 'user_available': 'null'
    #     }

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

# Define function to transcribe new data
def transcribe_call(id, download_path, transcription_path):
    """
    Given a call id, a download path (which should be the volume path) and a transcription path,
    the function transcribes the call and stores it in the transcription_path
    """
    # Creating the file name in text
    file_mp3 = f"{id}.mp3"
    file_txt = f"{id}.txt"
    file_path_recording = os.path.join(download_path, file_mp3)
    file_path_transcription = os.path.join(transcription_path, file_txt)

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

# COMMAND ----------

# Audio download path
download_path = '/Volumes/dev/aircall_raw/aircall_recordings'
# Transcriptions path
transcription_path = '/Volumes/dev/aircall_raw/call_transcriptions'
# Call aggregations path
aggregations_path = '/Volumes/dev/aircall_raw/aggregated_calls_by_account'

# Initiliaze new call data list of dicts
new_call_meta_list = []

for account in sf_calls['AccountId'].unique():
    # Initialize call counter
    call_counter = 0
    # Initialize headers
    headers = []
    transcriptions = []
    account_calls = sf_calls[sf_calls['AccountId'] == account]
    for i, call in account_calls.iterrows():
        # Construct the path to the audio and transcription file
        audio_file_name = f'{call["call_id"]}.mp3'
        transcription_file_name = f'{call["call_id"]}.txt'
        file_audio_path = os.path.join(download_path, audio_file_name)
        file_transcription_path = os.path.join(transcription_path, transcription_file_name)

        # Checking if file exists in downloads. If not, download it:
        if not(os.path.exists(file_audio_path)):
            download_call(call['call_id'], download_path)
        # Check if the file exists in transcriptions. If not, transcribe it:
        if not(os.path.exists(file_transcription_path)):
            transcribe_call(call['call_id'], download_path, transcription_path)

        call_counter+=1
        header = f'Call #{call_counter} - {call["CreatedDate"]}, duration {call["CallDurationInSeconds"]} seconds with {call["ds_full_name"]}'
        headers.append(header)
        transcriptions.append(file_transcription_path)


        # Declare the output file variables
        output_file_name = f'{account}.txt'
        output_file_path = os.path.join(aggregations_path, output_file_name)

        # Open the output file where we will write the combined content
        with open(output_file_path, 'w') as output_file:
            for header, file_path in zip(headers, transcriptions):
                # Write the header to the output file
                output_file.write(header + "\n\n")
                
                # Read the content of the current text file
                with open(file_path, 'r') as file:
                    content = file.read()
                
                # Write the file content under the header
                output_file.write(content + "\n\n")

# COMMAND ----------

# MAGIC %md
# MAGIC Write the prompt to OpenAI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summarize and save

# COMMAND ----------

def get_prompt_1(account_name="A"):
   prompt = f"""
   You are a specialized assistant tasked with processing and translating transcriptions from calls. The calls are transcribed in the orioginal language, each call separated by a header that follows this convention:
   Call #call_number - creation_date, duration n_seconds seconds with amName

   These are the parameters you are given:
   - **customerName**: {account_name}
   - **amName**: The name of the Metaloop agent that was in this particular call.

   Please do the following:

   1. **Format the Dialogue:**
      - Use the `amName` to label the account manager as `"amName (Metaloop)"`. You should identify the speaker from Metaloop from the transcription.
      - Use the `customerName` to label the customer as `"customerName (Customer)"`.
      - Structure the conversation in the format: `SpeakerName: dialogue`.

   2. **Translate the Dialogue:**
      - Translate the entire formatted conversation from German to English while maintaining accuracy and preserving the original meaning and nuances.

   3. **Provide the Final Output:**
      - Combine all the processed and translated calls into a single `.txt` file.
      - Name this file `[AccountId]_translated.txt`.
      - Start the file with this header:
      `"Call history between [amName] (Metaloop) and [customerName] ([companyName])"`

   Please ensure that:
   - The account manager and customer are correctly labeled using the extracted names.

   Once all tasks are complete, provide the final `.txt` file.
   """
   return prompt

def get_prompt_2():
   prompt = f"""
   From the translated conversation please extract the most important points to summarize the conversation. Make sure you identify and keep the nuances.
   """
   return prompt

# COMMAND ----------

# Translated file path
file_translated_path = '/Volumes/dev/aircall_raw/aggregated_calls_by_account_translated'

# Summary file path
file_summary_path = '/Volumes/dev/aircall_raw/call_summaries'

# Loop through aggregated calls to generate translations and summaries
for account in sf_calls['AccountId'].unique():

    # Get file to read without translation
    read_file_path = os.path.join(aggregations_path, f'{account}.txt')
    
    # Read the content of the current text file
    with open(read_file_path, 'r') as file:
        file_content = file.read()

    # Get account name
    account_name = sf_calls[sf_calls['AccountId'] == account]['ds_account'].iloc[0]
    prompt1 = get_prompt_1(account_name=account_name)
    # Send the file content along with the prompt to the API
    completion = open_ai_client.chat.completions.create(
    model="gpt-4o"
    , messages=[
        {"role": "system", "content": prompt1},
        {"role": "user", "content": file_content}
        ]
    # Trying to lock the seed so that the results are "more deterministic". This is not a guarantee and 
    # this feature is in beta. See the documentation 
    , seed=42 
    )

    # Get the generated text from the response
    output_translated_text = completion.choices[0].message.content.strip()

    # Construct output file
    output_translated_file = os.path.join(file_translated_path, f'{account}.txt')

    # Save the translation to a file
    with open(output_translated_file, 'w') as file:
        file.write(output_translated_text)

    # Get the second prompt
    prompt2 = get_prompt_2()

    # Send the file content along with the prompt to the API
    completion = open_ai_client.chat.completions.create(
    model="gpt-4o"
    , messages=[
        {"role": "system", "content": prompt2},
        {"role": "user", "content": output_translated_text}
        ]
    , seed=42 
    )

    # Get the generated text from the response
    output_summarized_text = completion.choices[0].message.content.strip()

    # Construct output file
    output_summarized_file = os.path.join(file_summary_path, f'{account}.txt')

    # Save the summary to a file
    with open(output_summarized_file, 'w') as file:
        file.write(output_summarized_text)




# COMMAND ----------

import os

# Translated file path
file_translated_path = '/Volumes/dev/aircall_raw/aggregated_calls_by_account_translated'

# Summary file path
file_summary_path = '/Volumes/dev/aircall_raw/call_summaries'

# Databricks domain
databricks_domain = 'https://dbc-9fa3b211-53b8.cloud.databricks.com'

def get_file_content(account_id, volume_path):
    file = f'{account_id}.txt'
    file_path = os.path.join(volume_path, file)
    with open(file_path, 'r') as file:
        file_content = file.read()
    return file_content

# Get unique account Id and Name and the link to the call summaries and translated aggregations files
accounts = sf_calls[['AccountId', 'ds_account']].drop_duplicates()
accounts['translation'] = accounts['AccountId'].apply(get_file_content, args=(file_translated_path, ))
accounts['summary'] =  accounts['AccountId'].apply(get_file_content, args=(file_summary_path, ))

# Create table in the catalog

# Define catalog to save the table to
database_name = 'dev'

# Define schema to save the table to
schema_name = 'aircall_raw'

# Define table name
table_name = 'dim_icp_account'

# Convert the pandas DataFrame to a PySpark DataFrame using the specified schema
spark_df = spark.createDataFrame(accounts)

# Define parameters
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
