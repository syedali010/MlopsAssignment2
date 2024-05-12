#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import json
import os
import subprocess

# Extraction: Web scraping function
def fetch_wikipedia_did_you_know():
    url = "https://en.wikipedia.org/wiki/Main_Page"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')
    dyk_section = soup.find('div', id='mp-dyk')

    # Find all list elements
    list_elements = dyk_section.find_all('li')

    # Extract the text from each list element and store it in a list
    did_you_know = [li.text for li in list_elements]

    items_to_exclude = ['Archive', 'Start a new article', 'Nominate an article']

    for item in items_to_exclude:
        did_you_know.remove(item)

    did_you_know = [sentence.replace('(pictured)', '') for sentence in did_you_know]

    prefix = "... that "
    did_you_know = [sentence[len(prefix):].rstrip('?').capitalize() if sentence.startswith(prefix) else sentence for
                    sentence in did_you_know]

    return did_you_know

# Transformation: Convert list to JSON
def convert_list_to_json(ti):
    extracted_data = ti.xcom_pull(task_ids='extract')
    transformed_data = json.dumps(extracted_data)
    ti.xcom_push(key='transformed_data', value=transformed_data)

# Loading: Save data to a file
def save_json_data(ti):
    data = ti.xcom_pull(key='transformed_data', task_ids='transform')
    with open('/mnt/d/airflow/data/ans_data.json', 'w') as file:
        file.write(data)

# DVC: Version and push the data
def version_and_push_data():
    os.chdir('/mnt/d/airflow/data')
    subprocess.run(['dvc', 'add', 'latest_data.json'])
    subprocess.run(['git', 'add', '.'])
    subprocess.run(['git', 'commit', '-m', 'Update data'])
    subprocess.run(['dvc', 'push'])
    subprocess.run(['git', 'push'])

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ali_etl_dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=fetch_wikipedia_did_you_know,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=convert_list_to_json,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=save_json_data,
    dag=dag,
)

version_task = PythonOperator(
    task_id='version_data',
    python_callable=version_and_push_data,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> version_task

