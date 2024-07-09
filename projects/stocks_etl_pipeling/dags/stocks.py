from airflow import DAG
from datetime import datetime, timedelta
import requests
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

with DAG(
    dag_id="stock_market",
    start_date=datetime(2024, 6, 18),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 3},
) as dag:

    @task()
    def hit_polygon_api(**context):
        stock_ticker = "AMZN"
        polygon_api_key = "PJ9fG4QVV9jlPbAhBu7oPJwLcbApws0c"
        ds = context.get("ds")

        url = f"<https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}>"
        response = requests.get(url)
        return response.json()

    hit_polygon_api()

    @task()
    def flatten_market_data(polygon_response, **context):
        columns = {
            "status": "closed",
            "from": context.get("ds"),
            "symbol": "AMZN",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
        }
        # Create a list to append the data to
        flattened_record = []
        for header_name, default_value in columns.items():
            # Append the data
            flattened_record.append(polygon_response.get(header_name, default_value))
        # Convert to a pandas DataFrame
        flattened_dataframe = pd.DataFrame([flattened_record], columns=columns.keys())
        return flattened_dataframe

    @task
    def load_market_data(flattened_dataframe):
        # Pull the connection
        market_database_hook = SqliteHook("market_database_conn")
        market_database_conn = market_database_hook.get_sqlalchemy_engine()
        # Load the table to SQLite, append if it exists
        flattened_dataframe.to_sql(
            name="market_data",
            con=market_database_conn,
            if_exists="append",
            index=False,
        )

    # Set dependencies between tasks
    raw_market_data = hit_polygon_api()
    transformed_market_data = flatten_market_data(raw_market_data)
    load_market_data(transformed_market_data)
