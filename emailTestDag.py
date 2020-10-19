import airflow

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.sensors import HttpSensor
import json
from datetime import timedelta
from datetime import datetime
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

args = {
    'owner': 'airflow',
    'email': ['test@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'max_active_runs':10
}

dag = DAG(dag_id='TEST_DAG', default_args=args, schedule_interval='@once')

new_cluster = {
    'spark_version': '4.0.x-scala2.11',
    'node_type_id': 'Standard_D16s_v3',
    'num_workers': 3,
    'spark_conf':{
        'spark.hadoop.javax.jdo.option.ConnectionDriverName':'org.postgresql.Driver',
        .....
    },
    'custom_tags':{
        'ApplicationName':'TEST',
        .....
    }
}

t1 = DatabricksSubmitRunOperator(
  task_id='t1',
  dag=dag,
  new_cluster=new_cluster,
  ......
)

t2 = SimpleHttpOperator(
    task_id='t2',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    method='POST',
    ........    
)

t2.set_upstream(t1)

t3 = SimpleHttpOperator(
    task_id='t3',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    method='POST',
   .....
 )

t3.set_upstream(t2)

AllTaskSuccess = EmailOperator (
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=["test@gmail.com"],
    subject="All Task completed successfully",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([t1, t2,t3])

t1Failed = EmailOperator (
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id="t1Failed",
    to=["test@gmail.com"],
    subject="T1 Failed",
    html_content='<h3>T1 Failed</h3>')

t1Failed.set_upstream([t1])

t2Failed = EmailOperator (
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id="t2Failed",
    to=["test@gmail.com"],
    subject="T2 Failed",
    html_content='<h3>T2 Failed</h3>')

t2Failed.set_upstream([t2])

t3Failed = EmailOperator (
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
    task_id="t3Failed",
    to=["test@gmail.com"],
    subject="T3 Failed",
    html_content='<h3>T3 Failed</h3>')

t3Failed.set_upstream([t3])
