# pylint: disable=import-error
from datetime import datetime, timedelta
import os
from os import path
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

PROJECT_PATH = path.abspath(path.join(path.dirname(__file__), '../..'))

default_args = {
    'owner': 'Filipe',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


dag = DAG('how-desafio-03',
          default_args=default_args,
          description='Extract, Load and transform data from OpenWeather API',
          start_date=days_ago(1),
          schedule="10 * * * *"
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

invoke_lambda_function = LambdaInvokeFunctionOperator(
    task_id="invoke_lambda_function",
    function_name='how-desafio-03'
)


end_operator = DummyOperator(task_id='Finish_execution',  dag=dag)

start_operator>>invoke_lambda_function>>end_operator