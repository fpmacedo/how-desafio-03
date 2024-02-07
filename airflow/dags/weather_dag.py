# pylint: disable=import-error
import time
from datetime import datetime, timedelta
import os
from os import path
import json
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.dates import days_ago
from datetime import date, timedelta
from airflow.models import Variable


PROJECT_PATH = path.abspath(path.join(path.dirname(__file__), '../..'))

default_args = {
    'owner': 'Filipe',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


dag = DAG('how-desafio-3',
          default_args=default_args,
          description='Extract, Load and transform data from OpenWeather API',
          start_date=days_ago(1),
          catchup=False,
          schedule="*/60 * * * *"
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

now = datetime.now() - timedelta(minutes=60)

rounded_hour = now.replace(minute=0, second=0, microsecond=0)

timestamp_unix = int(rounded_hour.timestamp())

invoke_lambda_function = LambdaInvokeFunctionOperator(
    task_id="invoke_lambda_function",
    function_name='how-desafio-3',
    payload=json.dumps({"date": str(timestamp_unix)}),
    invocation_type='Event'
)

JOB_FLOW_OVERRIDES = {
    "Name": "Weather ETL",
    "ReleaseLabel": "emr-6.12.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # We want our EMR cluster to have HDFS and Spark
    "BootstrapActions": [ 
       { 
          "Name": "great_expectations",
          "ScriptBootstrapAction": { 
             "Args": [ "string" ],
             "Path": "s3://how-desafio-3/bootstrap.sh"
          }
       }
    ],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # this lets us programmatically terminate the cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://how-desafio-3/logs/emr/",
}

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

SPARK_STEPS = [ # Note the params values are supplied to the operator
    {
        "Name": "Raw to trusted",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://how-desafio-3/spark/raw_to_trusted_script.py",
            ],
        },
    },
    {
        "Name": "Trusted to business",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://how-desafio-3/spark/trusted_to_business_script.py",
            ],
        },
    }
]

# Add your steps to the EMR cluster
add_steps = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id=create_emr_cluster.output,
    aws_conn_id="aws_default",
    steps=SPARK_STEPS
)

last_step = len(SPARK_STEPS) - 1

wait_for_step = EmrStepSensor(
    task_id="wait_for_step",
    job_flow_id=create_emr_cluster.output,
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default"
)

end_operator = DummyOperator(task_id='Finish_execution',  dag=dag)

start_operator>>invoke_lambda_function>>create_emr_cluster>>add_steps>>wait_for_step>>terminate_emr_cluster>>end_operator