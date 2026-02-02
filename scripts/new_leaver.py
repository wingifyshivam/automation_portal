import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
from datetime import datetime
import urllib.parse
import sys


file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Important Guides/customer_list.xlsx'
df = pd.read_excel(file_path)
db_username = os.getenv('DB_USERNAME', 'shivam.a')
db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
db_port = os.getenv('DB_PORT', '3306')

implementation_script = []
rollback_script = []
count = 0

for index, row in df.iterrows():
    db_host = row['ip_address']
    db_name = row['database_name']

    connection_string = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    # print(f"Connecting to database: {db_name}")
    # print("\n")
    
    # Create a database connection
    try:
        engine = create_engine(connection_string)
        connection = engine.connect()
        # print("Database connection successful.")
        # print("\n")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("\n")
        connection = None

    if connection:
        user_email = sys.argv[1]+'@lumeon.com'
        user_email_qinec = sys.argv[1]+'@qinec.com'
        user_email_hcat = sys.argv[1]+'@healthcatalyst.com'
        modified_by_function = sys.argv[2]

        query = f"select * from individual where (email like '%{user_email_qinec}%' or email like '%{user_email}%' or email like '%{user_email_hcat}%') and status = 'A';"
        data = pd.read_sql(text(query), engine)

        if not data.empty:
            count = count + 1
            print(f"There's a matching record in {db_name}")

            # Iterate over each row in the dataframe to construct the SQL UPDATE statements
            for _, row in data.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`individual` SET `status` = 'C', `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`individual_id` = '{row['individual_id']}');"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`individual` SET `status` = '{row['status']}', `last_modified_by` = '{row['last_modified_by']}', `last_modified` = '{row['last_modified']}', `modified_by_function` = '{row['modified_by_function']}' WHERE (`individual_id` = '{row['individual_id']}');"
                rollback_script.append(rollback_query)

    else:
        print("Unable to establish database connection.")

implementation_script.append('COMMIT;')
rollback_script.append('COMMIT;')
if count == 0:
    print('!!!! There is no matching record in any of the DBs. !!!!')
else:
    implementation_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Medigold/{}/implementation_script.sql'.format(modified_by_function)
    os.makedirs(os.path.dirname(implementation_file_path), exist_ok=True)
    with open(implementation_file_path, 'w') as sql_file:
        for statement in implementation_script:
            sql_file.write(statement + '\n')
    rollback_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Medigold/{}/rollback_script.sql'.format(modified_by_function)
    os.makedirs(os.path.dirname(rollback_file_path), exist_ok=True)
    with open(rollback_file_path, 'w') as sql_file:
        for statement in rollback_script:
            sql_file.write(statement + '\n')

    print(f"Implementation script saved as: " f"C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Medigold/{modified_by_function}/implementation_script.sql")
    print(f"Rollback script saved as: " f"C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Medigold/{modified_by_function}/rollback_script.sql")