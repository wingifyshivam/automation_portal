import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
import urllib.parse
from tabulate import tabulate
import sys
import json

# Database connection details
db_username = os.getenv('DB_USERNAME', 'shivam.a')
db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
db_host = os.getenv('DB_HOST', '172.20.131.33')
db_port = os.getenv('DB_PORT', '3306')
db_name = os.getenv('DB_NAME', 'newmedica_live')

# Connection string for MySQL
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
    # print(f"Error connecting to database: {e}")
    # print("\n")
    connection = None


if connection:
    modified_by_function = sys.argv[1]
    query = text(r"""
    SELECT appointment_id,
    cab_ubrn,
    a.last_modified_by last_modified_by_id,
    a.last_modified,
    a.modified_by_function,
    CONCAT_WS(' ', i.title, i.forename, i.surname) last_modified_by_name,
    i.email users
    FROM
        appointment a
            LEFT JOIN
        individual i ON a.last_modified_by = i.individual_id
    WHERE
        cab_ubrn LIKE '%\_\_\_\_\_\_\_\_\_\_\_\_\_\_\_%' ESCAPE '\\'
    GROUP BY a.appointment_id;
    """)
    data = pd.read_sql(query, engine)
    #df = pd.DataFrame(data, columns=['appointment_id', 'last_modified_by_id', 'last_modified_by_name'])
    df = pd.DataFrame(data, columns=['users'])
    # print(tabulate(df, headers='keys', tablefmt='psql'))
    data = pd.read_sql(query, engine)

    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            implementation_query = f"UPDATE `{db_name}`.`appointment` SET `cab_ubrn` = NULL, `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`appointment_id` = '{row['appointment_id']}');"
            implementation_script.append(implementation_query)
            rollback_query = f"UPDATE `{db_name}`.`appointment` SET `cab_ubrn` = '{row['cab_ubrn']}', `last_modified_by` = '{row['last_modified_by_id']}', `last_modified` = '{row['last_modified']}', `modified_by_function` = '{row['modified_by_function']}' WHERE (`appointment_id` = '{row['appointment_id']}');"
            rollback_script.append(rollback_query)
        implementation_script.append('COMMIT;')
        rollback_script.append('COMMIT;')
    
    except Exception as e:
        print(f"Error generating update/rollback statements")

    finally:
        connection.close()    

    # Optionally, write the rollback script to a file
    implementation_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Newmedica/{}/implementation_script.sql'.format(modified_by_function)
    os.makedirs(os.path.dirname(implementation_file_path), exist_ok=True)
    with open(implementation_file_path, 'w') as sql_file:
        for statement in implementation_script:
            sql_file.write(statement + '\n')
    rollback_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Newmedica/{}/rollback_script.sql'.format(modified_by_function)
    os.makedirs(os.path.dirname(rollback_file_path), exist_ok=True)
    with open(rollback_file_path, 'w') as sql_file:
        for statement in rollback_script:
            sql_file.write(statement + '\n')

    # print("\n")
    # print(f"Implementation script saved as: " f"C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Newmedica/{modified_by_function}/implementation_script.sql")
    # print("\n")
    # print(f"Rollback script saved as: " f"C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Newmedica/{modified_by_function}/rollback_script.sql")

    result = {
        "folder_name": db_name,
        "implementation_path": implementation_file_path,
        "rollback_path": rollback_file_path
    }

    print(json.dumps(result))

else:
    print("Unable to establish database connection.")