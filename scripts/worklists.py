import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
from datetime import datetime
import urllib.parse
import sys
import json

client = sys.argv[1]

if client == 'BUPA UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.129.55')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'bupa_uat')
elif client == 'BUPA Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.129.12')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'bupa_production_v6')
elif client == 'Nuffield Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.130.107')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'nuffield_live')
elif client == 'Nuffield UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.130.93')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'nuffield_uat')
elif client == 'Newmedica Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.131.33')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'newmedica_live')
elif client == 'Newmedica UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.131.93')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'newmedica_uat')
elif client == 'Onebright Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.63')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_live')
elif client == 'Onebright UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.91')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_uat')
else:
    print('Invalid client!!')

db_to_folder = {
    'bupa_production_v6': 'BUPA',
    'bupa_uat': 'BUPA',
    'nuffield_live': 'Nuffield',
    'nuffield_uat': 'Nuffield',
    'newmedica_live': 'Newmedica',
    'newmedica_uat': 'Newmedica',
    'onebright_live': 'Onebright',
    'onebright_uat': 'Onebright'
}
folder_name = db_to_folder.get(db_name, 'UnknownDB')

# Connection string for MySQL
connection_string = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
# print(f"Connecting to database: {db_name}")
# print("\n")

# Create a database connection
try:
    engine = create_engine(connection_string)
    connection = engine.connect()
    # print(f"Successfully connected to database: {db_name}")
    # print("\n")
except Exception as e:
    # print(f"Error connecting to database: {e}")
    # print("\n")
    connection = None

if connection:
    modified_by_function = sys.argv[3]
    base_folder = f'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{folder_name}/{modified_by_function}'
    
    #query = f"select * from dashboard_view where dashboard_workflow_id = 58 and dashboard_view_name = 'Cataract Post-Op to Book';"
    #query = sys.argv[2]
    dashboard_set_name = sys.argv[2]
    dashboard_workflow_name = sys.argv[3]
    dashboard_view_name = sys.argv[4]
    query = f"select * from dashboard_view where dashboard_view_name = {dashboard_view_name} and dashboard_workflow_id = (select dashboard_workflow_id from dashboard_workflow where dashboard_workflow_name = {dashboard_workflow_name} where dashboard_set_name = {dashboard_set_name}));"

    query = """
    SELECT *
    FROM dashboard_view
    WHERE dashboard_view_name = :view_name
    AND dashboard_workflow_id = (
        SELECT dashboard_workflow_id
        FROM dashboard_workflow
        WHERE dashboard_workflow_name = :workflow_name
        AND dashboard_set_id = (
            SELECT dashboard_set_id
            from dashboard_set
            WHERE dashboard_set_name = :set_name)
    )
    """

    params = {
        "view_name": dashboard_view_name,
        "workflow_name": dashboard_workflow_name,
        "set_name": dashboard_set_name
    }

    data = pd.read_sql(text(query), engine, params=params)

    table_html = data.to_html(
        classes="table table-striped",
        index=False,
        border=0
    )

    output = {
        "table": table_html
    }

    print(json.dumps(output))

else:
    print("Unable to establish database connection.")