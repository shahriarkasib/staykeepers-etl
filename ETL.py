#importing libraries
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

#read datasets from the sources
sales_orders = pd.read_excel('task data tables - Data Engineer.xlsx', sheet_name = 'Sales Orders')
customers = pd.read_excel('task data tables - Data Engineer.xlsx', sheet_name = 'Customers')
regions = pd.read_excel('task data tables - Data Engineer.xlsx', sheet_name = 'Regions')
products = pd.read_excel('task data tables - Data Engineer.xlsx', sheet_name = 'Products')

#removing spaces from column names as it does not follow column naming convention of Bigquery
sales_orders.columns = sales_orders.columns.str.replace(' ', '_')
customers.columns = customers.columns.str.replace(' ', '_')
regions.columns = regions.columns.str.replace(' ', '_')
products.columns = products.columns.str.replace(' ', '_')

#connect with bigquery
credentials = service_account.Credentials.from_service_account_file('staykeeper_bigquery_credential.json')
project_id = 'dataproject-359115'
client = bigquery.Client(credentials = credentials, project = project_id)

#create your data here
#upload sales orders data to Bigquery
sales_orders.to_gbq(project_id = project_id,
                    destination_table = 'staykeeper.sales_orders',
                    credentials = credentials,
                    if_exists = 'replace')


#upload customers data to Bigquery
customers.to_gbq(project_id = project_id,
                 destination_table = 'staykeeper.customers',
                 credentials = credentials,
                 if_exists = 'replace')

#upload regions data to Bigquery
regions.to_gbq(project_id = project_id,
               destination_table = 'staykeeper.regions',
               credentials = credentials,
               if_exists = 'replace')

#upload products data to Bigquery
products.to_gbq(project_id = project_id,
                destination_table = 'staykeeper.products',
                credentials = credentials,
                if_exists = 'replace')