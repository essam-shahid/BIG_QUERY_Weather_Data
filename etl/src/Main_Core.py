import os
import os.path
from sys import exit
import pandas as pd
import json
import api
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime

def Extract_Weather():



    #The following date function to extract date-1 api data once added in pipeline
    date_today = datetime.date.today()
    no_of_days = datetime.timedelta(days=2)
    dt = date_today - no_of_days

    print('Loading for day')

    # store the URL in url as
    # parameter for requests.get

    URL_Link = 'https://api.weatherapi.com/v1/history.json?key=894e63bd79be4214aa580838223107&q=Amsterdam&dt=' + str(dt)

      # store the response of URL
    response = requests.get(URL_Link)

    #Alert to check if response code successfully executed
    if response.status_code == 200:
        print('Success!')
    elif response.status_code == 404:
        print('Not Found.')

    # store the response in json format and returns the value
    data_response = response.json()

    #Returns the response
    return (data_response)


def get_loading_df(data_response):

    #Flattened json will be stored in the below variable
    flat_json={}

    #Recursive function created for json flattening
    #Input will be json object and key will be for reference used as columns

    def flatten(data_response,key=""):
        if type(data_response) is dict:
            for a in data_response:
                flatten(data_response[a],key+a+'_')
        elif type(data_response) is list:
            i = 0
            for a in data_response:
                flatten(a, key + str(i) + '_')
                i+=1
        else:
            flat_json[key[:-1]] = data_response

    flatten(data_response)

    #Once the json file has been flattened, we will normaliz the json
    df = pd.json_normalize(flat_json)

    return (df)

def load_weather_to_GCP_staging():

    #Get the API data from Extract_Weather and load into a dataframe
    weather_data=Extract_Weather()
    loading_df=get_loading_df(weather_data)
    table_id='bol-assignment-357814.Bol_Case_Dataset.weather_data_stg'

    #Use the service account Key for credentials
    credentials = service_account.Credentials.from_service_account_file("C:/Users\essam.shahid\Desktop\BOL_Analytics_Engineer_Case\Assignment_2\etl\src\keyfile.json")

        #Connect to bigquery
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
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

def Final_Loading_to_core_Layer():


    # Adding latest data from staging into core layer
    date_today = datetime.date.today()
    table_id = 'bol-assignment-357814.Bol_Case_Dataset.weather_data'

    # Use the service account Key for credentials
    credentials = service_account.Credentials.from_service_account_file(
        "C:/Users\essam.shahid\Desktop\BOL_Analytics_Engineer_Case\Assignment_2\etl\src\keyfile.json")

    # Connect to bigquery
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )



    job_config = bigquery.QueryJobConfig()
    # Set the destination table
    Final_query="Insert into `bol-assignment-357814.Bol_Case_Dataset.weather_data`" \
                 "select   CAST(forecast_forecastday_0_date AS date) as weather_date, location_name,   location_region,location_country,  " \
                 " forecast_forecastday_0_day_maxtemp_c AS day_maxtemp_celsius,   forecast_forecastday_0_day_mintemp_c AS day_mintemp_celsius,  " \
                 " forecast_forecastday_0_day_avgtemp_c AS day_avgtemp_celsius,   forecast_forecastday_0_hour_0_temp_c AS Hour_0_temp,   forecast_forecastday_0_hour_1_temp_c AS Hour_1_temp," \
                 "   forecast_forecastday_0_hour_2_temp_c AS Hour_2_temp,   forecast_forecastday_0_hour_3_temp_c AS Hour_3_temp,   forecast_forecastday_0_hour_4_temp_c AS Hour_4_temp, " \
                 "  forecast_forecastday_0_hour_5_temp_c AS Hour_5_temp,   forecast_forecastday_0_hour_6_temp_c AS Hour_6_temp,   forecast_forecastday_0_hour_7_temp_c AS Hour_7_temp,  " \
                 " forecast_forecastday_0_hour_8_temp_c AS Hour_8_temp,   forecast_forecastday_0_hour_9_temp_c AS Hour_9_temp,   forecast_forecastday_0_hour_10_temp_c AS Hour_10_temp,  " \
                 " forecast_forecastday_0_hour_11_temp_c AS Hour_11_temp,   forecast_forecastday_0_hour_12_temp_c AS Hour_12_temp,   forecast_forecastday_0_hour_13_temp_c AS Hour_13_temp, " \
                 "  forecast_forecastday_0_hour_14_temp_c AS Hour_14_temp,   forecast_forecastday_0_hour_15_temp_c AS Hour_15_temp,   forecast_forecastday_0_hour_16_temp_c AS Hour_16_temp, " \
                 "  forecast_forecastday_0_hour_17_temp_c AS Hour_17_temp,   forecast_forecastday_0_hour_18_temp_c AS Hour_18_temp,   forecast_forecastday_0_hour_19_temp_c AS Hour_19_temp,   " \
                 "forecast_forecastday_0_hour_20_temp_c AS Hour_20_temp,   forecast_forecastday_0_hour_21_temp_c AS Hour_21_temp,   forecast_forecastday_0_hour_22_temp_c AS Hour_22_temp, " \
                 "  forecast_forecastday_0_hour_23_temp_c AS Hour_23_temp,   CAST(SUBSTR(location_localtime,1,10) AS date) load_date FROM bol-assignment-357814.Bol_Case_Dataset.weather_data_stg"

    query_job = client.query(Final_query,job_config=job_config)
    query_job.result()  # Waits for the query to finish
    print("Query results loaded to table {}".format(table_ref.path))


if __name__ == '__main__':
    load_weather_to_GCP_staging()
    Final_Loading_to_core_Layer()