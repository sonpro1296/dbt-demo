import pendulum

from airflow.models.dag import DAG
from airflow.decorators import dag, task


with DAG(
    "son_dag",
    default_args={},
    description="DAG tutorial",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    
    @task.bash
    def move_schema():
        return "cd /home/sonvd/dbt-demo && dbt run -s stock_raw"

    @task.bash
    def dbt_run():
        return 'cd /home/sonvd/dbt-demo && dbt run -s +get_monthly_prices'
    
    @task.bash
    def dbt_test():
        return 'cd /home/sonvd/dbt-demo && dbt test'


    move_schema() >> dbt_run() >> dbt_test()

