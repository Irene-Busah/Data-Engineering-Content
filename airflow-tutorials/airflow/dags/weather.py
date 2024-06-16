from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import sqlite3


def extract_weather_data():
    api_key = "4706a144e8220c712887fb70e080f2a8"
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    city_names = [
        "Lagos",
        "Cairo",
        "Nairobi",
        "Johannesburg",
        "Cape Town",
        "Accra",
        "Casablanca",
        "Addis Ababa",
        "Algiers",
        "Kampala",
        "Dakar",
        "Abidjan",
        "Luanda",
        "Khartoum",
        "Dar es Salaam",
        "Kinshasa",
        "Rabat",
        "Tunis",
        "Douala",
        "Harare",
        "Alexandria",
        "Giza",
        "Port Harcourt",
        "Ibadan",
        "Mogadishu",
        "Tripoli",
        "Bamako",
        "Ouagadougou",
        "Niamey",
        "Maputo",
        "Lusaka",
        "Freetown",
        "Conakry",
        "Libreville",
        "Windhoek",
        "Antananarivo",
        "Nouakchott",
        "Banjul",
        "Bujumbura",
        "Asmara",
        "Juba",
        "Brazzaville",
        "Victoria",
        "Saint-Denis",
        "Porto-Novo",
        "Lome",
        "Malabo",
        "Praia",
        "Moroni",
        "Djibouti",
    ]
    weather_data_list = []
    for city_name in city_names:
        params = {"q": city_name, "appid": api_key, "units": "metric"}
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            weather_data = response.json()
            weather_data_list.append(
                {
                    "City": city_name,
                    "Temperature (Celsius)": weather_data["main"]["temp"],
                    "Weather": weather_data["weather"][0]["description"],
                    "Humidity (%)": weather_data["main"]["humidity"],
                    "Pressure": weather_data["main"]["pressure"],
                    "Wind Speed": weather_data["wind"]["speed"],
                    "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        else:
            print(f"Error retrieving data for {city_name}: {response.status_code}")

    if weather_data_list:
        df = pd.DataFrame(weather_data_list)
        output_file = "/opt/airflow/dags/weather_data.csv"
        df.to_csv(output_file, index=False)
        print(f"Weather data extracted and saved to '{output_file}'")
        return True
    else:
        print("No weather data retrieved.")
        return False


def transform_data():
    input_csv_file = "/opt/airflow/dags/weather_data.csv"
    df = pd.read_csv(input_csv_file)
    df["Date"] = pd.to_datetime(df["Date"])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    output_file = "/opt/airflow/dags/transformed_weather_data.csv"
    df.to_csv(output_file, index=False)
    return True


def load_data_to_sqlite():
    input_csv_file = "/opt/airflow/dags/transformed_weather_data.csv"
    df = pd.read_csv(input_csv_file)
    db_name = "/opt/airflow/dags/weather_data.sqlite"
    conn = sqlite3.connect(db_name)
    df.to_sql("weather", conn, if_exists="replace", index=False)
    conn.close()
    return True


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 15),
}

dag = DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="This is a simple ETL pipeline that retrieves data from a weather database",
    schedule_interval="@daily",
)

extract_task = PythonOperator(
    task_id="extract_weather_data", python_callable=extract_weather_data, dag=dag
)

transform_task = PythonOperator(
    task_id="transform_weather_data", python_callable=transform_data, dag=dag
)

load_task = PythonOperator(
    task_id="load_weather_data", python_callable=load_data_to_sqlite, dag=dag
)

extract_task >> transform_task >> load_task
