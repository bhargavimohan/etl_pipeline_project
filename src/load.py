import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def load_data(kwargs):

    connection_str = f"snowflake://{kwargs["snowflake_user"]}:{kwargs["snowflake_password"]}@{kwargs["snowflake_account"]}/{kwargs["snowflake_database"]}/{kwargs["snowflake_schema"]}?warehouse={kwargs["snowflake_warehouse"]}&role={kwargs["snowflake_role"]}"  # Create the Snowflake connection string

    engine = create_engine(
        connection_str
    )  # Create a SQLAlchemy engine to connect to Snowflake
    df = pd.read_csv(kwargs["processed_data_path"])
    try:
        # Write the DataFrame to Snowflake
        df.to_sql(
            "PROCESSED_DATA", con=engine, index=False, if_exists="replace"
        )  # or 'append'
        print("Data successfully loaded to Snowflake!")

    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")


# import os
# from dotenv import load_dotenv
# import snowflake.connector

# # Load environment variables from .env file
# load_dotenv()

# # Fetch credentials from the .env file
# USER = os.getenv("SNOWFLAKE_USER")
# PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
# ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# DATABASE = os.getenv("SNOWFLAKE_DATABASE")
# SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
# WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
# ROLE = os.getenv("SNOWFLAKE_ROLE")


# # Create a connection using the Snowflake connector
# conn = snowflake.connector.connect(
#     user=USER,
#     password=PASSWORD,
#     account=ACCOUNT,
#     warehouse=WAREHOUSE,
#     database=DATABASE,
#     schema=SCHEMA,
#     role=ROLE,
# )

# # Test the connection
# try:
#     # Execute a simple query
#     cursor = conn.cursor()
#     cursor.execute("SELECT CURRENT_VERSION()")
#     version = cursor.fetchone()
#     print(f"Connection successful! Snowflake version: {version[0]}")
# finally:
#     cursor.close()
#     conn.close()
