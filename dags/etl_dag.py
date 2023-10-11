from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Quang Dinh',
    'start_date': days_ago(0) # make start date in the past
}

#defining the dag object
dag = DAG(
    dag_id='etl_show_dag',
    default_args=args,
    # schedule_interval='@daily' #to make this workflow happen every day
    schedule_interval='30 12,23 * * *' #to make this workflow happen every 5 minutes
)

#assigning the task for our dag to do
with dag:
    opr_import_logs = BashOperator(
        task_id='import_logs',
        bash_command='PYTHONPATH=/apps/etl-show-mm/etl_python /usr/bin/python3 /apps/etl-show-mm/app/import.py -env /apps/etl-show-mm/.env',
    )
    opr_insert_dim = BashOperator(
        task_id='insert_dim',
        bash_command='PYTHONPATH=/apps/etl-show-mm/etl_python /usr/bin/python3 /apps/etl-show-mm/app/main.py -env /apps/etl-show-mm/.env -a dim',
    )
    opr_run_transforms = BashOperator(
        task_id='run_transforms',
        bash_command='PYTHONPATH=/apps/etl-show-mm/etl_python /usr/bin/python3 /apps/etl-show-mm/app/main.py -env /apps/etl-show-mm/.env -a daily',
    )
    opr_update_dim = BashOperator(
        task_id='update_dim_daily',
        bash_command='PYTHONPATH=/apps/etl-show-mm/etl_python /usr/bin/python3 /apps/etl-show-mm/app/main.py -env /apps/etl-show-mm/.env -a dim_update',
    )

    opr_import_logs >> opr_insert_dim
    opr_insert_dim >> opr_run_transforms
    opr_run_transforms >> opr_update_dim