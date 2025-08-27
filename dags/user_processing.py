from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue

@dag
def user_processing():

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests

        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else: 
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    @task
    def extract_user(fake_user):
        # fake_user = ti.xcom_pull(task_id="is_api_available")
        
        # import requests
        # response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        # fake_user = response.json()

        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }

    @task
    def process_user(user_info):
        import csv

        user_info = {
            "id": "123",
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
        }

        with open("/tmp/user_info.csv", "w", newline="") as f:
            writter = csv.DictWriter(f, fieldnames=user_info.keys())
            writter.writeheader()
            writter.writerow(user_info)
    
    fake_user=is_api_available()
    user_info=extract_user(fake_user)
    process_user(user_info)

user_processing()