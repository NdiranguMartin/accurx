import os
import json
from db_connection import execute_query
from progressbar import progressbar
import time

# creating a table to store the history of runs made 
create_run_history_table = """
    CREATE TABLE IF NOT EXISTS accurx.run_history
    (
        run_start_ts BIGINT,
        run_end_ts BIGINT,
        records_read INT,
        records_write INT,
        existing_records INT
    )
        """
execute_query(query=create_run_history_table)

def camel_to_snake(string):
    # This function converts camel case to snake case
    return ''.join(['_' + i.lower() if i.isupper()
                    else i for i in string]).lstrip('_')


# ---- create table ------


run_start_ts = int(time.time())

# create a table to store data from the json objects 
create_opportunities_table = """
    CREATE TABLE IF NOT EXISTS salesforce.opportunities
    (
        -- Client opportunity info
        opportunity_id VARCHAR(256),
        opportunity_name VARCHAR(256),
        last_updated_date VARCHAR(256),
        company_size VARCHAR(256),
        industry VARCHAR(256),
        -- Sales person name
        account_owner VARCHAR(256),
        -- Id of the client account
        account_id VARCHAR(226),
        locale VARCHAR(10),
        -- Producttier access (Silver, Gold, Diamond)
        access_tier VARCHAR(100),
        -- Monthly, Quarterly, Annually
        payment_frequency VARCHAR(256),
        -- Cancelled or blank
        payment_status VARCHAR(256),
        -- Outbound patient message
        product1 VARCHAR(256),
        value1 NUMERIC(16, 2) ,
        -- “Close Won”, “Lost” or blank
        state1 VARCHAR(50) ,
        -- Inbound patient message
        product2 VARCHAR(256) ,
        value2 NUMERIC(16, 2) ,
        -- “Close Won”, “Lost” or blank
        state2 VARCHAR(50) ,
        file_dt VARCHAR(50) 
    );
"""

execute_query(query=create_opportunities_table)

# Get existing records and store them in a set 
existing_dts = execute_query(
    query="SELECT file_dt, opportunity_id FROM salesforce.opportunities",
    has_results=True,
)
existing_records_count = len(existing_dts)

existing_dts = ['_'.join(row) for row in existing_dts]
existing_records = set(existing_dts)

# looping over the contents of the data folder to read the json files
main_path = 'ingestion/data/'
daily_paths = [day for day in os.listdir(main_path) if not day.startswith('.')]
daily_paths.sort()


records_read = 0
records_write = 0
for day in progressbar(daily_paths):
    day_path = f'{main_path}{day}/'
    daily_files = [f for f in os.listdir(day_path) if not f.startswith('.')]

    for f in daily_files:
        with open(f'{day_path}{f}') as f:
            for obj in f:
                records_read += 1
                record = json.loads(obj)
                record_id = day + '_' + record['opportunityId']

                if record_id not in existing_records:
                    # process a row only if it is not in the DB 
                    col_names = list(record.keys()) + ['file_dt']
                    col_names = tuple([camel_to_snake(col) for col in col_names])
                    col_names = ', '.join(col_names)
                    row = list(record.values()) + [f'{day}']
                    record_to_insert = (tuple(row))

                    place_holders = ['%s'] * len(record_to_insert)
                    place_holders = ','.join(place_holders)

                    insert_query = f'INSERT INTO salesforce.opportunities({col_names}) VALUES ({place_holders})'
                    execute_query(query=insert_query, batch_insert=True, records=record_to_insert)
                    records_write += 1


run_end_ts = int(time.time()) 
insert_run_history = f'INSERT INTO accurx.run_history(run_start_ts, run_end_ts, records_read, records_write, existing_records) VALUES (%s, %s, %s, %s, %s)'
execute_query(query=insert_run_history, batch_insert=True, records=(run_start_ts, run_end_ts, records_read, records_write, existing_records_count))


report = (
    f'Total records found: {records_read}\n'
    f'Already existing records: {existing_records_count}\n'
    f'New records inserted: {records_write}\n'
    f'Time taken: {(run_end_ts - run_start_ts)} seconds\n'
)
print(report)