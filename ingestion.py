import os
import json
from db_connection import execute_query

# ---- create table ------
create_table = """
    CREATE TABLE accurx.opportunities
    (
        -- Client opportunity info
        opportunity_id VARCHAR(256) NOT NULL,
        opportunity_name VARCHAR(256) NOT NULL,
        last_updated TIMESTAMPTZ NOT NULL,
        company_size VARCHAR(256) NOT NULL,
        industry VARCHAR(256) NOT NULL,
        -- Sales person name
        account_owner VARCHAR(256) NOT NULL,
        -- Id of the client account
        account_id VARCHAR(18) NOT NULL,
        locale VARCHAR(10) NOT NULL,
        -- Producttier access (Silver, Gold, Diamond)
        access_tier VARCHAR(100) NOT NULL,
        -- Monthly, Quarterly, Annually
        payment_frequency VARCHAR(256) NOT NULL,
        -- Cancelled or blank
        payment_status VARCHAR(256) NOT NULL,

        -- Outbound patient message
        product1 VARCHAR(256) NOT NULL,
        value1 NUMERIC(16, 2) NOT NULL,
        -- “Close Won”, “Lost” or blank
        state1 VARCHAR(50) NOT NULL,
        -- Inbound patient message
        product2 VARCHAR(256) NULL,
        value2 NUMERIC(16, 2) NULL,
        -- “Close Won”, “Lost” or blank
        state2 VARCHAR(50) NULL,
        file_dt VARCHAR(50) NOT NULL
    );
"""

execute_query(query = create_table)
existing_dts = execute_query(
    query="SELECT file_dt FROM accurx.opportunities", 
    has_results=True
    )

print(existing_dts)



main_path = 'data/'
daily_paths = [day for day in os.listdir(main_path) if not day.startswith('.')]
daily_paths.sort()
daily_data = {}

for day in daily_paths[:2]:
    if day not in existing_dts:
        day_path = f'{main_path}{day}/'
        daily_files = [f for f in os.listdir(day_path) if not f.startswith('.')]

        for f in daily_files:
            with open(f'{day_path}{f}') as daily_file:
                file_contents = daily_file.read()

                parsed_json = json.loads(file_contents)
                col_names = [parsed_json.keys()] + ['file_dt']
                col_names = tuple(col_names)
                col_values = [parsed_json.values()] + [f'{day}']
                col_values = tuple(col_values)
                insert = f'INSERT INTO accux.opportunities{col_names} VALUES {col_values}'
                print(insert)

# print(daily_data)