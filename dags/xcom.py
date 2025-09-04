from airflow.sdk import dag, task, Context
from typing import Any

@dag
def xcom_dag():

    @task
    def t1() -> dict[str, Any]:
        val = 42
        my_sentence = 'Hello World!'
        # context['ti'].xcom_push(key='my_key', value=val)
        return {
            "my_val": val,
            "my_sentence": my_sentence
        }

    @task
    def t2(data: dict[str, Any]):
        # val = context['ti'].xcom_pull(task_ids='t1', key='my_key')
        print(data['my_val'])
        print(data['my_sentence'])

    val = t1() 
    t2(val)

xcom_dag()