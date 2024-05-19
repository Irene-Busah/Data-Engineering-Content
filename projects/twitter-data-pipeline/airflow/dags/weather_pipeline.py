import requests
import pandas as pd
from datetime import datetime
import sqlite3


def extract_weather_data(units="metric"):
    """
    Extract weather data for multiple cities from the OpenWeatherMap API,
    convert it to a Pandas DataFrame, and save it to a CSV file.

    Args:
        api_key (str): Your OpenWeatherMap API key.
        city_names (list): List of city names (e.g., ["London", "New York"]).
        units (str): Units for temperature measurement. Default is 'metric' (Celsius).

    Returns:
        bool: True if data extraction and saving were successful, False otherwise.
    """

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
        params = {"q": city_name, "appid": api_key, "units": units}
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
        # Convert weather data list to DataFrame
        df = pd.DataFrame(weather_data_list)
        # Save DataFrame to CSV file
        output_file = "weather_data.csv"
        df.to_csv(output_file, index=False)
        print(f"Weather data extracted and saved to '{output_file}'")
        return True
    else:
        print("No weather data retrieved.")
        return False


# the transform method
def transform_data():
    """
    Transforms the extracted weather data by cleaning and preparing it for loading.

    Parameters:
    - input_csv_file (str): The path to the CSV file containing the extracted weather data.

    Returns:
    - DataFrame: A cleaned and transformed pandas DataFrame.
    """

    input_csv_file = "weather_data.csv"

    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv_file)

    # Convert the date column to datetime format
    df["date"] = pd.to_datetime(df["date"])

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Reset the index
    df.reset_index(drop=True, inplace=True)
    df.to_csv("transformed_weather_data.csv", index=False)

    return True


# the load method
def load_data_to_sqlite():
    """
    Loads the transformed data into an SQLite database.

    Parameters:
    - transformed_df (DataFrame): The transformed pandas DataFrame.
    - db_name (str): The name of the SQLite database file (default is 'weather_data.db').
    """

    transformed_df = pd.read_csv("transformed_weather_data.csv")

    # Connect to the SQLite database (or create it if it doesn't exist)
    db_name = "weather_data.sqlite"
    conn = sqlite3.connect(db_name)

    # Load the DataFrame into the database
    transformed_df.to_sql("weather", conn, if_exists="replace", index=False)

    # Close the database connection
    conn.close()
    return True
