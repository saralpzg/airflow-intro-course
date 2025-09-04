from airflow.sdk import dag, task

@dag
def branch():

    @task
    def a():
        return 1

    @task.branch
    def b(val: int):
        if val == 1:
            return ["equal_1", "run_if_1"]
        return "different_1"

    @task
    def equal_1(val: int):
        print(f"Value is equal to {val}")

    @task
    def different_1(val: int):
        print(f"Value is different than 1: {val}")

    @task
    def run_if_1():
        print("This also runs if value is 1")

    val = a() 
    b(val) >> [equal_1(val), different_1(val), run_if_1()] # list enables the branching

branch()