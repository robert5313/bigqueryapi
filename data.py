from dotenv import load_dotenv

from google.cloud import bigquery
import os
import requests
from google.oauth2 import service_account
import pandas_gbq as gbq
import pandas as pd
from google.cloud import storage

load_dotenv()
#my credentials for google cloud
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

#consuming the rest api here
data = requests.get('https://datausa.io/api/data?drilldowns=State&measures=Population&year=2020,2019,2018,2017,2016,2015')
json = data.json()
df = pd.DataFrame(json['data'])

#displaying all the data in the rest api
pd.set_option('display.max_rows', None)

#renaming the columns to make it accessible by bigquery 
dict = {'ID State': 'id_state',
        'State': 'state',
        'ID Year': 'id_year',
        'Population': 'population',
        'Slug State': 'slug_state'}

df.rename(columns=dict,
          inplace=True)

#the name of the table_id = 'my_dataset.my_table' project_id = "my-project" following each other
gbq.to_gbq(df, 'testingdata.test_new', 'techie-blogger-58ef8', if_exists='append')
print(df)