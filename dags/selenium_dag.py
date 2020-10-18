import os
from airflow.models import DAG
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
import airflow.hooks.S3_hook
from selenium_scripts.get_movement_range import get_movement_range
from datetime import datetime, timedelta
import logging


class ExtendedPythonOperator(PythonOperator):
    '''
    extending the python operator so macros
    get processed for the op_kwargs field.
    '''
    template_fields = ('templates_dict', 'op_kwargs')


def upload_file_to_S3(file_name, key, bucket_name):
    '''
    Uploads a local file to s3.
    '''
    hook = airflow.hooks.S3_hook.S3Hook('S3_conn_id')
    hook.load_file(file_name, key, bucket_name)
    logging.info(
        'loaded {} to s3 bucket:{} as {}'.format(file_name, bucket_name, key))


def remove_file(file_name, local_path):
    '''
    Removes a local file.
    '''
    file_path = os.path.join(local_path, file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)
        logging.info('removed {}'.format(file_path))


date = '{{ ds_nodash }}'
file_name = 'episode_{}.mp3'.format(date)
bucket_name = 'wake_up_to_money'
key = os.path.join(bucket_name, file_name)
cwd = os.getcwd()
local_downloads = os.path.join(cwd, 'downloads')

default_args = {
    'owner': 'harry_daniels',
    # 'wait_for_downstream': True,
    'start_date': datetime(2020, 10, 17),
    'retries': 3,
    'retries_delay': timedelta(minutes=5)
    }

dag = DAG('selenium_example_dag',
          schedule_interval='0 7 * * *',
          default_args=default_args)

get_csv = SeleniumOperator(
    script=get_movement_range,
    script_args=["https://web.facebook.com/geoinsights-portal/downloads/vector/?id=642750926308152&ds={}&extra%5Bcrisis_name%5D=IDN_gadm_2",
                 "favianhazman@yahoo.co.id",
                 "september05"],
    task_id='get_movement_range',
    dag=dag)

upload_podcast_to_s3 = ExtendedPythonOperator(
    python_callable=upload_file_to_S3,
    op_kwargs={'file_name': file_name,
               'key': file_name,
               'bucket_name': bucket_name},
    task_id='upload_podcast_to_s3',
    dag=dag)

remove_local_podcast = ExtendedPythonOperator(
    python_callable=remove_file,
    op_kwargs={'file_name': file_name,
               'local_path': local_downloads},
    task_id='remove_local_podcast',
    dag=dag)

get_csv >> upload_podcast_to_s3
upload_podcast_to_s3 >> remove_local_podcast
