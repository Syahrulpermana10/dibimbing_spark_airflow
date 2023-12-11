from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Test for spark submit",
    start_date=days_ago(1),
)

# Define the SparkSubmitOperator
spark_task = SparkSubmitOperator(
    application="/spark-scripts/spark-example.py",
    conn_id="spark_tgs",
    task_id="spark_submit_task",
    dag=spark_dag,
)

# Define BashOperators for additional analyses and outputs
bash_task_1 = BashOperator(
    task_id='run_additional_analysis_1',
    bash_command='spark-submit /spark-scripts/spark-example.py',
    dag=spark_dag,
)

bash_task_2 = BashOperator(
    task_id='run_additional_analysis_2',
    bash_command='spark-submit /spark-scripts/spark-example.py',
    dag=spark_dag,
)

# Output results to CSV using BashOperators
output_path = "/output"
bash_output_churn = BashOperator(
    task_id='output_churn_results',
    bash_command=f'mv /spark-scripts/churned_customers/*.csv {output_path}/churned_customers.csv',
    dag=spark_dag,
)

bash_output_retention = BashOperator(
    task_id='output_retention_results',
    bash_command=f'mv /spark-scripts/retained_customers/*.csv {output_path}/retained_customers.csv',
    dag=spark_dag,
)

# Dependencies
spark_task >> bash_task_1
spark_task >> bash_task_2
bash_task_1 >> bash_output_churn
bash_task_2 >> bash_output_retention
