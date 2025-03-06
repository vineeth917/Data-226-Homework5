from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import pendulum

# Default arguments for the DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Function to return a Snowflake connection cursor
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

# Defining the DAG using the context manager
with DAG(
    dag_id='stock_price_full_refresh_delete',  # Unique identifier for the DAG
    schedule='30 * * * *',                        # DAG will run daily
    start_date=datetime(2024,9,25),  # Start date is 1 day ago
    catchup=False,                            # No catchup for missed runs
    default_args=default_args,                # Set retry and delay settings
    description='Full refresh of stock data for the last 90 days using DELETE'
) as dag:

    @task
    def fetch_last_90_days_data():
        symbols = ["IBM"]
        api_key = Variable.get("Alpha_Vantage_API")
        results = []

        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
            response = requests.get(url)
            data = response.json()

            if "Time Series (Daily)" not in data:
                print(f"Error fetching data for {symbol}:", data)
                continue

            time_series = data["Time Series (Daily)"]
            end_date = datetime.today().date()
            start_date = end_date - timedelta(days=90)

            for date, values in time_series.items():
                date_obj = datetime.strptime(date, "%Y-%m-%d").date()
                if start_date <= date_obj <= end_date:
                    results.append({
                        "symbol": symbol,
                        "date": date,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": int(values["5. volume"])
                    })

        df = pd.DataFrame(results)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        return df.to_dict()

    @task
    def transform(records):
        df = pd.DataFrame(records)
        return df

    @task
    def load(df, target_table):
        cur = return_snowflake_conn()
        try:
            cur.execute("BEGIN;")
            cur.execute(f"DELETE FROM {target_table};")
            print("All rows deleted successfully.")

            for index, row in df.iterrows():
                sql = """
                INSERT INTO {target_table} (date, open, high, low, close, volume, symbol)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cur.execute(sql, (
                    row['date'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row['symbol']
                ))

            cur.execute("COMMIT;")
            print("Data inserted successfully.")

        except Exception as e:
            cur.execute("ROLLBACK;")
            print(f"Error occurred: {e}")
            raise e

        finally:
            cur.close()

    # Define task dependencies
    target_table = "COUNTRY.RAW.Alpha"
    records = fetch_last_90_days_data()
    df = transform(records)
    load(df, target_table)

