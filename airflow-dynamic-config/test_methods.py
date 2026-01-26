from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Method 1: Context manager
with DAG("test1", start_date=datetime(2024, 1, 1)) as dag1:
    EmptyOperator(task_id="task1")

# Method 2: Direct assignment
dag2 = DAG("test2", start_date=datetime(2024, 1, 1))
EmptyOperator(task_id="task2", dag=dag2)

# Method 3: globals().update()
def make_dag():
    dag = DAG("test3", start_date=datetime(2024, 1, 1))
    EmptyOperator(task_id="task3", dag=dag)
    return dag

globals().update({"test3_dag": make_dag()})

