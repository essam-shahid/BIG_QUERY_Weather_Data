import pandas as pd
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime


def Extract_Weather():
    # The following date function to extract date-1 api data once added in pipeline
    date_today = datetime.date.today()
    no_of_days = datetime.timedelta(days=1)

    dt = date_today - no_of_days
    print('Loading for date: ' + str(dt))


    # store the URL in url as
    # parameter for requests.get

    URL_Link = 'https://api.weatherapi.com/v1/history.json?key=894e63bd79be4214aa580838223107&q=Amsterdam&dt=' + str(dt)

    # store the response of URL
    response = requests.get(URL_Link)

    # Alert to check if response code successfully executed
    if response.status_code == 200:
        print('Response Code Received! Success!')
    elif response.status_code == 404:
        print('Response Code Not Found.')

    # store the response in json format and returns the value
    data_response = response.json()

    # Returns the response
    return (data_response)


def get_loading_df(data_response):
    # Flattened json will be stored in the below variable
    flat_json = {}

    # Recursive function created for json flattening
    # Input will be json object and key will be for reference used as columns

    def flatten(data_response, key=""):
        if type(data_response) is dict:
            for a in data_response:
                flatten(data_response[a], key + a + '_')
        elif type(data_response) is list:
            i = 0
            for a in data_response:
                flatten(a, key + str(i) + '_')
                i += 1
        else:
            flat_json[key[:-1]] = data_response

    flatten(data_response)

    # Once the json file has been flattened, we will normaliz the json
    df = pd.json_normalize(flat_json)

    return (df)


def load_weather_to_GCP_staging():

    # Get the API data from Extract_Weather and load into a dataframe
    weather_data = Extract_Weather()
    loading_df = get_loading_df(weather_data)
    table_id = 'bol-assignment-357814.Bol_Case_Dataset.weather_data_stg'

    # Use the service account Key for credentials

    credentials = service_account.Credentials.from_service_account_file("/app/keyfile.json")
    print("Connection to BigQuery Made")

    # Connect to bigquery


    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        loading_df, table_id, job_config=job_config
    )

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
          )
        )



if __name__ == '__main__':
    print('ETL Pipeline Started!')

    load_weather_to_GCP_staging()

    print('ETL Pipeline Finished!')

