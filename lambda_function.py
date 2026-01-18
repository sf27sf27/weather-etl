"""
AWS Lambda ETL function for Open-Meteo weather data.
Incrementally loads weather records into PostgreSQL based on date cursor.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

import openmeteo_requests
import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values
from retry_requests import retry

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Weather API configuration
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_PARAMS = {
    "latitude": 33.87531062590638,
    "longitude": -84.50210615154822,
    "minutely_15": [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "rain",
        "sunshine_duration",
        "precipitation",
        "dew_point_2m",
        "wind_speed_10m",
    ],
    "timezone": "America/New_York",
    "past_days": 1,  # Fetch last day to ensure we catch any missed data
    "forecast_days": 0,  # No forecast data - historical only
}


def get_db_connection():
    """Create and return a PostgreSQL database connection."""
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


def check_table_exists(conn) -> bool:
    """Check if the sensor_project.weather_data table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'sensor_project' 
                AND table_name = 'weather_data'
            );
        """)
        exists = cur.fetchone()[0]
    
    if exists:
        logger.info("weather_data table exists")
    else:
        logger.error("weather_data table does not exist! Please create it first.")
    
    return exists


def get_latest_date_cursor(conn) -> Optional[datetime]:
    """Get the most recent date from the database to use as cursor."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(date) FROM sensor_project.weather_data")
        result = cur.fetchone()[0]
    
    if result:
        logger.info(f"Latest date cursor from DB: {result}")
    else:
        logger.info("No existing records found, will sync all available data")
    
    return result


def fetch_weather_data() -> pd.DataFrame:
    """Fetch weather data from Open-Meteo API."""
    # Setup API client with retry on error
    session = requests.Session()
    retry_session = retry(session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    responses = openmeteo.weather_api(WEATHER_API_URL, params=WEATHER_PARAMS)
    response = responses[0]

    logger.info(f"Fetched data for coordinates: {response.Latitude()}°N {response.Longitude()}°E")

    # Process minutely_15 data
    minutely_15 = response.Minutely15()
    
    # Extract all variables in order
    data = {
        "date": pd.date_range(
            start=pd.to_datetime(minutely_15.Time(), unit="s", utc=True),
            end=pd.to_datetime(minutely_15.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=minutely_15.Interval()),
            inclusive="left",
        ),
        "temperature_2m": minutely_15.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": minutely_15.Variables(1).ValuesAsNumpy(),
        "apparent_temperature": minutely_15.Variables(2).ValuesAsNumpy(),
        "rain": minutely_15.Variables(3).ValuesAsNumpy(),
        "sunshine_duration": minutely_15.Variables(4).ValuesAsNumpy(),
        "precipitation": minutely_15.Variables(5).ValuesAsNumpy(),
        "dew_point_2m": minutely_15.Variables(6).ValuesAsNumpy(),
        "wind_speed_10m": minutely_15.Variables(7).ValuesAsNumpy(),
    }

    df = pd.DataFrame(data)
    logger.info(f"Fetched {len(df)} records from API")
    return df


def filter_new_records(df: pd.DataFrame, cursor: Optional[datetime]) -> pd.DataFrame:
    """Filter dataframe to only include records newer than the cursor and exclude future dates."""
    # Exclude any future dates - we only want historical data
    now = datetime.now(timezone.utc)
    df = df[df["date"] <= now]
    logger.info(f"Filtered out future dates, {len(df)} records remaining")
    
    if cursor is None:
        logger.info("No cursor, returning all historical records")
        return df
    
    # Ensure cursor is timezone-aware for comparison
    if cursor.tzinfo is None:
        cursor = cursor.replace(tzinfo=timezone.utc)
    
    # Filter for records strictly after the cursor
    new_records = df[df["date"] > cursor]
    logger.info(f"Filtered to {len(new_records)} new records after cursor {cursor}")
    return new_records


def insert_records(conn, df: pd.DataFrame) -> int:
    """Insert new records into the database using batch insert."""
    if df.empty:
        logger.info("No new records to insert")
        return 0

    # Prepare data for insertion
    columns = [
        "date",
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "rain",
        "sunshine_duration",
        "precipitation",
        "dew_point_2m",
        "wind_speed_10m",
    ]
    
    # Convert DataFrame to list of tuples, handling NaN values
    records = []
    for _, row in df.iterrows():
        record = tuple(
            None if pd.isna(row[col]) else row[col] for col in columns
        )
        records.append(record)

    # Use INSERT with ON CONFLICT DO NOTHING to handle duplicates gracefully
    insert_sql = """
    INSERT INTO sensor_project.weather_data (
        date, temperature_2m, relative_humidity_2m, apparent_temperature,
        rain, sunshine_duration, precipitation, dew_point_2m, wind_speed_10m
    ) VALUES %s
    ON CONFLICT (date) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, records)
        inserted_count = cur.rowcount
    
    conn.commit()
    logger.info(f"Inserted {inserted_count} new records into database")
    return inserted_count


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    Triggered every 5 minutes by CloudWatch Events to sync weather data.
    """
    logger.info("Starting weather data ETL process")
    
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        logger.info("Connected to database")

        # Verify table exists
        if not check_table_exists(conn):
            return {
                "statusCode": 500,
                "body": {
                    "message": "ETL process failed",
                    "error": "weather_data table does not exist. Please create it first.",
                },
            }

        # Get the latest date cursor
        cursor = get_latest_date_cursor(conn)

        # Fetch weather data from API
        weather_df = fetch_weather_data()

        # Filter to only new records
        new_records_df = filter_new_records(weather_df, cursor)

        # Insert new records
        inserted_count = insert_records(conn, new_records_df)

        result = {
            "statusCode": 200,
            "body": {
                "message": "ETL process completed successfully",
                "records_fetched": len(weather_df),
                "records_inserted": inserted_count,
                "latest_cursor": str(cursor) if cursor else None,
            },
        }
        logger.info(f"ETL process completed: {result['body']}")
        return result

    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": {
                "message": "ETL process failed",
                "error": str(e),
            },
        }
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


# For local testing
if __name__ == "__main__":
    from dotenv import load_dotenv
    
    load_dotenv()
    result = lambda_handler({}, None)
    print(result)
