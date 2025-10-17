# Standard imports
import os.path
import pandas as pd

# Import API clients
from api_clients.earthquake_client import get_earthquake_events
from api_clients.station_client import generate_stations_list
from api_clients.weather_client import get_weather_forcast

# Main function
def main():

    # Pull station list as dataframe
    print(f'Pulling station list ...')
    stations = station_list()

    # Request earthquake data
    print(f'Pulling earthquake data ...')
    get_earthquake_events()

    # Split the dataframe into sections groups for weather request
    print(f'Pulling weather data ...')

    # Pull latitude/longitude from dataframe
    pairs = list(zip(stations['lat'], stations['long']))

    # For each station request weather, loop handled in weather_client
    get_weather_forcast(pairs)

# Station list generator
# Only need to pull once unlike other events, as it is purely station and location
def station_list():

    # Define path for station list
    stations = f'../data/raw/station-coordinates.parquet'

    # Check if file exists
    if not os.path.exists(stations):
        generate_stations_list()

    # Read in the file
    df = pd.read_parquet(stations)
    return df

if __name__ == "__main__":
    main()