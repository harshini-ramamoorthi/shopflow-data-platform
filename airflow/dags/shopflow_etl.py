"""
ShopFlow Data Warehouse ETL Pipeline

Orchestrates the existing ShopFlow scripts:
PostgreSQL -> Raw CSV -> PySpark staging -> Warehouse dimensions and fact
-> Analytics outputs -> Data validation.

This DAG is manual-trigger only while the pipeline is being tested.
"""

from datetime import datetime
from pathlib import Path
import os
import subprocess

from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook


PROJECT_DIR = Path("/opt/shopflow")
PYTHON = "/home/airflow/shopflow-venv/bin/python"

DBT_PROJECT_DIR = PROJECT_DIR / "dbt_shopflow"
DBT_PROFILES_DIR = Path("/opt/airflow/config")
DBT_TARGET_PATH = Path("/tmp/dbt-target")
DBT_LOG_PATH = Path("/tmp/dbt-logs")


def run_shopflow_script(script_name: str) -> None:
    """Run one existing ShopFlow script from the project directory."""

    script_path = PROJECT_DIR / script_name

    if not script_path.is_file():
        raise FileNotFoundError(f"ShopFlow script not found: {script_path}")

    # Read database credentials from the Airflow connection.
    connection = BaseHook.get_connection("shopflow_postgres")

    if not all([connection.host, connection.schema,
                connection.login, connection.password]):
        raise ValueError(
            "The shopflow_postgres connection is missing required fields."
        )

    # Pass credentials to the script as environment variables.
    # The password is not printed or written into the DAG.
    env = os.environ.copy()
    env.update({
        "DB_HOST": connection.host,
        "DB_PORT": str(connection.port or 5432),
        "DB_NAME": connection.schema,
        "DB_USER": connection.login,
        "DB_PASSWORD": connection.password,
    })
    
    env["PYTHONPATH"] = str(PROJECT_DIR) + os.pathsep + env.get("PYTHONPATH", "")
        
    command = [PYTHON, str(script_path)]

    print(f"Running ShopFlow script: {script_name}")

    subprocess.run(
        command,
        cwd=PROJECT_DIR,
        env=env,
        check=True,
    )


def run_dbt(command: str) -> None:
    dbt_command = [
        "/home/airflow/shopflow-venv/bin/dbt",
        command,
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--target-path", str(DBT_TARGET_PATH),
        "--log-path", str(DBT_LOG_PATH),
    ]

    env = os.environ.copy()
    env["SHOPFLOW_DATA_PATH"] = "/opt/shopflow/data"

    print(f"Running dbt {command}")
    subprocess.run(
        dbt_command,
        cwd=DBT_PROJECT_DIR,
        env=env,
        check=True,
    )
    
@dag(
    dag_id="shopflow_etl_pipeline",
    description=(
        "Extracts ShopFlow PostgreSQL data, transforms it with PySpark, "
        "builds the warehouse and analytics outputs, and runs validations."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["shopflow", "etl", "pyspark", "data-warehouse"],
)
def shopflow_etl_pipeline():

    @task
    def run_script(script_name: str) -> None:
        run_shopflow_script(script_name)

        # 1. Extract source tables from PostgreSQL into raw CSV files.
    extract_raw = run_script.override(task_id="extract_raw")(
        "ingestion/extract_raw.py"
    )

    # 2. Transform raw CSV files into staging Parquet datasets.
    transform_customers = run_script.override(task_id="transform_customers")(
        "spark/transform_customers.py"
    )
    transform_products = run_script.override(task_id="transform_products")(
        "spark/transform_products.py"
    )
    transform_orders = run_script.override(task_id="transform_orders")(
        "spark/transform_orders.py"
    )
    transform_order_items = run_script.override(task_id="transform_order_items")(
        "spark/transform_order_items.py"
    )
    transform_payments = run_script.override(task_id="transform_payments")(
        "spark/transform_payments.py"
    )

    # 3. Build warehouse dimensions.
    build_dim_customer = run_script.override(task_id="build_dim_customer")(
        "spark/build_dim_customer.py"
    )
    build_dim_product = run_script.override(task_id="build_dim_product")(
        "spark/build_dim_product.py"
    )
    build_dim_payment = run_script.override(task_id="build_dim_payment")(
        "spark/build_dim_payment.py"
    )
    build_dim_date = run_script.override(task_id="build_dim_date")(
        "spark/build_dim_date.py"
    )

    # 4. Build the central fact table after its inputs are ready.
    build_fact_orders = run_script.override(task_id="build_fact_orders")(
        "spark/build_fact_orders.py"
    )

    # 5. Build analytics datasets after the fact table is ready.
    build_daily_sales = run_script.override(task_id="build_daily_sales")(
        "spark/build_daily_sales.py"
    )
    build_product_performance = run_script.override(
        task_id="build_product_performance"
    )("spark/build_product_performance.py")
    build_customer_sales = run_script.override(task_id="build_customer_sales")(
        "spark/build_customer_sales.py"
    )
    build_category_performance = run_script.override(
        task_id="build_category_performance"
    )("spark/build_category_performance.py")
    build_payment_analysis = run_script.override(task_id="build_payment_analysis")(
        "spark/build_payment_analysis.py"
    )
    
        # 5.5. Run dbt models and dbt data tests.
    @task
    def run_dbt_task(command: str) -> None:
        run_dbt(command)

    dbt_run = run_dbt_task.override(task_id="dbt_run")("run")
    dbt_test = run_dbt_task.override(task_id="dbt_test")("test")
    

    # 6. Run warehouse and analytics validations.
    validate_warehouse = run_script.override(task_id="validate_warehouse")(
        "spark/validate_warehouse.py"
    )
    check_customers_without_sales = run_script.override(
        task_id="check_customers_without_sales"
    )("spark/check_customers_without_sales.py")
    validate_analytics = run_script.override(task_id="validate_analytics")(
        "spark/validate_analytics.py"
    )

        # Define the task dependencies.
    # Run Spark transformations sequentially to avoid multiple
    # Spark JVMs competing for resources inside the Airflow worker.

    extract_raw >> transform_customers
    transform_customers >> transform_products
    transform_products >> transform_orders
    transform_orders >> transform_order_items
    transform_order_items >> transform_payments

    # Build warehouse dimensions after all staging transformations.
    transform_payments >> [
        build_dim_customer,
        build_dim_product,
        build_dim_payment,
        build_dim_date,
    ]

    # Build the central fact table after warehouse dimensions are ready.
    [
        build_dim_customer,
        build_dim_product,
        build_dim_payment,
        build_dim_date,
    ] >> build_fact_orders

    # Existing PySpark analytics remain in the pipeline.
    build_fact_orders >> [
        build_daily_sales,
        build_product_performance,
        build_customer_sales,
        build_category_performance,
        build_payment_analysis,
    ]

    # Validate the warehouse before running dbt.
    build_fact_orders >> validate_warehouse
    validate_warehouse >> check_customers_without_sales


    validate_warehouse >> dbt_run >> dbt_test

    # Validate the existing PySpark analytics.
    [
        build_daily_sales,
        build_product_performance,
        build_customer_sales,
        build_category_performance,
        build_payment_analysis,
    ] >> validate_analytics


shopflow_etl_pipeline()
