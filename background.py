import asyncio
import aioredis
import aiohttp
import json
import logging
from datetime import datetime, timedelta
import sqlite3
import psycopg2
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_URL = "https://api.spectator.earth/acquisition-plan/"
API_KEY = os.getenv('SPECTATOR_API_KEY')
SATELLITES = ['Landsat-8', 'Landsat-9']


def get_the_last_date(satellite: str):
    """
    Retrieve the last available data date for the given satellite.

    Args:
        satellite (str): The name of the satellite.

    Returns:
        str: The last date with available data for the satellite, or None if not found.
    """
    try:
        conn = sqlite3.connect("db.sqlite3")
        cursor = conn.cursor()

        with psycopg2.connect(
            user="postgres",
            password="postgres",
            host="db",
            port="5432",
            database="postgres"
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT has_info_date
                    FROM api_acqusitiondatesinfo
                    WHERE satellite = %s
                    ORDER BY has_info_date DESC
                    LIMIT 1;
                ''', (satellite,))
                last_date = cursor.fetchone()

                return last_date[0] if last_date else None
    except Exception as e:
        logging.error(f"Error retrieving the last date for {satellite}: {e}")
        return None


async def fetch_and_save_data(redis: aioredis.Redis):
    """
    Fetch data from the external API and save it to both the database and Redis.

    Args:
        redis (aioredis.Redis): Redis connection object.
    """
    while True:
        async with aiohttp.ClientSession() as session:
            for satellite in SATELLITES:
                date = get_the_last_date(satellite)
                if not date:
                    date = datetime.now()
                else:
                    date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                
                for _ in range(100):
                    date += timedelta(days=1)
                    date_str = date.strftime("%Y-%m-%dT00:00:00Z")
                    needed_date = date.strftime("%Y-%m-%d")

                    params = {
                        "api_key": API_KEY,
                        "satellites": satellite,
                        "datetime": date_str
                    }

                    try:
                        async with session.get(API_URL, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                if data.get("features"):
                                    await save_to_db(data["features"], date, satellite)
                                    await redis.set(needed_date, json.dumps(data['features']), ex=604800*3)
                                    logging.info(f"Data for satellite {satellite} for date {needed_date} saved.")
                                else:
                                    logging.info(f"No data for satellite {satellite} on date {needed_date}.")
                            else:
                                logging.error(f"Error {response.status} for satellite {satellite} and date {date_str}")
                    except Exception as e:
                        logging.error(f"Error requesting API data for {satellite}: {e}")

        await asyncio.sleep(3600*3)


async def save_to_db(features: list, datetime: datetime, satellite: str):
    """
    Save acquisition data to the PostgreSQL database.

    Args:
        features (list): List of features returned from the API.
        datetime (datetime): The date of acquisition.
        satellite (str): The name of the satellite.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            user='postgres',
            password='postgres',
            database='postgres',
            host='db',
            port='5432'
        )
        with conn:
            with conn.cursor() as cursor:
                date_ = datetime.strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute('''
                    INSERT INTO api_acqusitiondatesinfo (satellite, has_info_date)
                    VALUES (%s, %s)
                ''', (satellite, date_))
                logging.info(f"Data for satellite {satellite} for date {date_} saved to DB.")
    except Exception as e:
        logging.error(f"Error saving data to DB for {satellite}: {e}")
    finally:
        if conn:
            conn.close()


def build_square(lon: float, lat: float, delta: float):
    """
    Build a polygon (geometry) based on the coordinates and delta value.

    Args:
        lon (float): Longitude of the center point.
        lat (float): Latitude of the center point.
        delta (float): Distance to form the square.

    Returns:
        dict: A GeoJSON polygon representing the square.
    """
    c1 = [lon + delta, lat + delta]
    c2 = [lon + delta, lat - delta]
    c3 = [lon - delta, lat - delta]
    c4 = [lon - delta, lat + delta]
    geometry = {"type": "Polygon", "coordinates": [[c1, c2, c3, c4, c1]]}
    return geometry


def convert_to_rfc3339(date_str: str):
    """
    Convert a date range to RFC3339 format.

    Args:
        date_str (str): A string containing the date range (e.g., "2023-01-01/2023-01-31").

    Returns:
        str: Date range in RFC3339 format.
    """
    dates = date_str.split('/')
    extra_part = 'T00:00:00Z'
    return f'{dates[0]}{extra_part}/{dates[1]}{extra_part}'


async def get_landsat_items(lon: float, lat: float, time_range: str, min_cloud=0, max_cloud=100):
    """
    Retrieve Landsat scenes based on coordinates, time range, and cloud cover percentage.

    Args:
        lon (float): Longitude of the area.
        lat (float): Latitude of the area.
        time_range (str): Date range in the format "YYYY-MM-DD/YYYY-MM-DD".
        min_cloud (int, optional): Minimum cloud cover percentage. Defaults to 0.
        max_cloud (int, optional): Maximum cloud cover percentage. Defaults to 100.

    Returns:
        dict: Features from the STAC server or a message if no data is found.
    """
    url = "https://landsatlook.usgs.gov/stac-server/search"
    geometry = build_square(lon, lat, 0.004)
    payload = {
        "intersects": geometry,
        "datetime": convert_to_rfc3339(time_range),
        "query": {
            "eo:cloud_cover": {
                "gte": min_cloud,
                "lte": max_cloud,
            }
        },
        "collections": ["landsat-c2l2-sr"]
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["features"] if data['features'] else {'message': 'not found'}
                else:
                    logging.error(f"Error requesting STAC data: {response.status}")
                    return None
        except Exception as e:
            logging.error(f"Error retrieving data for coordinates ({lon}, {lat}): {e}")
            return None


async def worker(redis: aioredis.Redis):
    """
    Worker process that processes requests from the Redis queue.

    Args:
        redis (aioredis.Redis): Redis connection object.
    """
    while True:
        try:
            request_data = await redis.lpop('request_queue')
            if request_data:
                request = json.loads(request_data)
                request_id = request['request_id']
                lon = request['lon']
                lat = request['lat']
                min_cloud = int(request['min_cloud'])
                max_cloud = int(request['max_cloud'])
                time_range = request['time_range']

                items = await get_landsat_items(lon, lat, time_range, min_cloud, max_cloud)
                await redis.set(f"result:{request_id}", json.dumps(items), ex=120)
                logging.info(f"Request {request_id} processed.")
        except Exception as e:
            logging.error(f"Error processing queue: {e}")

        await asyncio.sleep(1)


async def main():
    """
    Main function that starts the worker and data-fetching processes.
    """
    redis = await aioredis.from_url("redis://redis:6379/0")
    task1 = asyncio.create_task(worker(redis))
    task2 = asyncio.create_task(fetch_and_save_data(redis))
    await task1
    await task2


if __name__ == "__main__":
    asyncio.run(main())
