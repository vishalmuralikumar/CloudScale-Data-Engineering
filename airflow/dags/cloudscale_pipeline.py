from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="cloudscale_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["cloudscale", "data-engineering"],
) as dag:

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    data_quality = BashOperator(
        task_id="data_quality",
        bash_command=(
            "python /workspace/src/"
            "cloudscale_data_engineering/data_quality/clean_orders.py"
        ),
    )

    spark_transform = BashOperator(
    task_id="spark_transform",
    bash_command=(
        "docker exec cloudscale-spark "
        "/opt/spark/bin/spark-submit "
        "/workspace/src/"
        "cloudscale_data_engineering/spark/orders_transform.py"
    ),
)
    

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        "cd /workspace/cloudscale_dbt && "
        "dbt run --profiles-dir /home/airflow/.dbt"
    ),
)
    

    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=(
        "cd /workspace/cloudscale_dbt && "
        "dbt test --profiles-dir /home/airflow/.dbt"
    ),
)
    

    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete"
    )

    start_pipeline >> data_quality >> spark_transform >> dbt_run >> dbt_test >> pipeline_complete