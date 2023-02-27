import os
import json
from db_connection import execute_query


def camel_to_snake(str):
	return ''.join(['_'+i.lower() if i.isupper()
			else i for i in str]).lstrip('_')

# ---- create table ------

# execute_query(
#     query="DROP TABLE accurx.opportunities",
#     )
create_table = """
    CREATE TABLE IF NOT EXISTS accurx.opportunities
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

execute_query(query = create_table)
existing_dts = execute_query(
    query="SELECT file_dt, opportunity_id FROM accurx.opportunities",
    has_results=True,
    )

existing_dts = ['_'.join(row) for row in existing_dts]
existing_records = tuple(existing_dts)


main_path = 'data/'
daily_paths = [day for day in os.listdir(main_path) if not day.startswith('.')]
daily_paths.sort()

for day in daily_paths:
    print('----------------------------------')
    print(f'Begin reading {day}')
    day_path = f'{main_path}{day}/'
    daily_files = [f for f in os.listdir(day_path) if not f.startswith('.')]

    for f in daily_files:
        with open(f'{day_path}{f}') as f:
            for obj in f:
                record = json.loads(obj)
                record_id =  day + '_' + record['opportunityId']

                if record_id not in existing_records:
                    col_names = list(record.keys()) + ['file_dt']
                    col_names = tuple([camel_to_snake(col) for col in col_names])
                    col_names = ', '.join(col_names)
                    row = list(record.values()) + [f'{day}']
                    record_to_insert = (tuple(row))

                    place_holders = ['%s'] * len(record_to_insert)
                    place_holders = ','.join(place_holders)

                    insert_query = f'INSERT INTO accurx.opportunities({col_names}) VALUES ({place_holders})'
                    # print(insert_query, record_to_insert)
                    execute_query(query=insert_query, batch_insert=True, records=record_to_insert)
                else:
                    print(f'{record_id} already exists')
    
    print(f'successfully worked on {day}')
    print('----------------------------------')
