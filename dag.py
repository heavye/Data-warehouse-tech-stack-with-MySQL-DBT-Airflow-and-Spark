from datetime import delta, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner' : 'airflow',
    'start_date' : airflow.utils.dates.days_ago(2),
    # 'end_date' : datetime(2020, 12, 30),
    'depends_on_past' : False,
    'email' : ['airflow@example.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    # If a task falils retry once after waiting 5 minitues
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5),
}

#Instansiating DAG
dag = DAG(
    'tutorial',
    default_args= default_args,
    description='A simple tutorial DAG',
    #continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)

#Tasks
t1 = BashOperator(
    task_id = 'print date',
    bash_command= 'date',
    dag= dag,
)

t2 = BashOperator(
    task_id = 'sleep',
    depends_on_past = False,
    bash_command='sleep 5',
    dag = dag,
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param}}"
{% endfor %}
"""

t3 = BashOperator(
    task_id = 'templated',
    depends_on_past = False,
    bash_command= templated_command,
    params= {'my_param':'Parameter I passed in'},
    dag = dag,
)

#Setting up the dependancy
#This means that t2 will depend on t1
# running successfull to run
t1.set_downstream(t2)

#similat to above where t3 will depend on t1
t3.set_upstream(t1)