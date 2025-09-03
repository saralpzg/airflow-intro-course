from airflow.sdk import asset, Asset, Context

@asset(
    schedule="@daily",
    uri="https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json"
)
def user(self) -> dict[str]:
    import requests

    r = requests.get(self.uri)
    return r.json()

# Command to check assets enabled:
# airflow assets list

# Command to materialize asset:
# airflow assets materialize --name user

@asset(
    schedule=user
)
def user_personal_info(user: Asset, context: Context) -> dict[str]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True
    )
    return user_data['personalInfo']

@asset(
    schedule=user
)
def user_account_details(user: Asset, context: Context) -> dict[str]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True
    )
    return user_data['accountDetails']

# Materialize two assets at once
@asset.multi(
    schedule=user,
    outlets=[
        Asset(name="user_personal_info_bis"),
        Asset(name="user_account_details_bis")
    ]
)
def user_info(user: Asset, context: Context) -> list[dict[str]]:
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,
        task_ids=user.name,
        include_prior_dates=True
    )
    return [user_data['personalInfo'], user_data['accountDetails']]
