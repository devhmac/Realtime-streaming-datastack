from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json

default_args = {
  "owner": "devhmac",
  "start_date": datetime(2023,11,12,14,00)
}

# hit api get endpoint
def get_data():
  import requests
  res = requests.get("https://randomuser.me/api/")
  res = res.json()
  first = res['results'][0]
  return first

# format/flatten data response 
def format_data(res):
  data = {}
  
  data['first_name'] = res['name']['first']
  data['last_name'] = res['name']['last']
  data['gender'] = res['gender']
  data['address'] = str(res['location']["street"]["number"]) + ' '+ res['location']["street"]["name"]+', ' + res['location']["city"] + ', ' +  res['location']["state"] + ', ' + res['location']["country"]
  data['postal_code'] = res['location']['postcode']
  data['username'] = res['login']['username']
  data['email'] = res['email']
  
  return data


# call helper functions  
def stream_data():
  
  data = get_data()
  print(data)
  data = format_data(data)
  print(json.dumps(data, indent=4))


with DAG('user_automation',
         default_args = default_args,
         schedule='@daily',
         catchup=False) as dag:
  
  streaming_task = PythonOperator(
    task_id = 'stream_data_from_api',
    python_callable=stream_data
  )
  
  stream_data()