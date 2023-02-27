import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv

load_dotenv()

def execute_query(query, has_results = False, batch_insert = False, records = []):
    try:
        connection = psycopg2.connect(
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            database=os.getenv("DATABASE"),
            )

        cursor = connection.cursor()

        if batch_insert:
            cursor.execute(query, records)
            print(cursor.rowcount, " records inserted successfully")
        else:
            cursor.execute(query)
            print("Executed successfully")

        if has_results:
            ans = cursor.fetchall()

            return ans


        connection.commit()

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")