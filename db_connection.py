import psycopg2
from psycopg2 import Error
import os
from dotenv import load_dotenv

load_dotenv()

def execute_query(query, has_results = False):
    try:
        connection = psycopg2.connect(
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT"),
            database=os.getenv("DATABASE"),
            )

        cursor = connection.cursor()
        cursor.execute(query)

        if has_results:          
            ans =cursor.fetchall()
            dict_result = []
            for row in ans:
                dict_result.append(dict(row))

        print("Executed successfully")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            connection.commit()
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
        
        if has_results:
            return dict_result