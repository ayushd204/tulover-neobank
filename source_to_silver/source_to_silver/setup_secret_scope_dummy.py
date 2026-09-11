# Databricks notebook source
sqlserver_host = "server_name"     #some remote windows server instead of localhost in laptop
sqlserver_port = "port"
sqlserver_database = "db_name"
sqlserver_user = "user"
sqlserver_password = "password"

# Secret scope name (will be created if not exists) ; it is similar to a secret folder name
secret_scope_name = "scope_name"

# Secret key name (single secret containing full JSON)
secret_key_name = "sqlserver-connection-json"


# COMMAND ----------

import json

connection_config = {
    "host": sqlserver_host,
    "port": sqlserver_port,
    "database": sqlserver_database,
    "user": sqlserver_user,
    "password": sqlserver_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
#the driver is built in within databricks platform for SQL server 
# if the same thing is tried with Oracle, it fails cause the driver has to be installed manually in the cluster itself and databricks does not have driver installed !

connection_json = json.dumps(connection_config)

print("Generated JSON Configuration:")
print(connection_json)

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext() 
# metadata about current notebook -- present CONTEXT;

api_url  = ctx.apiUrl().getOrElse(None)     # url of the current workspace
api_token = ctx.apiToken().getOrElse(None)  # personal access token for this session

print(api_url)
print(api_token)  # handle securely, do not log in real code
#the api token is printed 'redacted'

# COMMAND ----------

import requests
import json

DATABRICKS_INSTANCE = api_url  # the current workspace URL
DATABRICKS_TOKEN = api_token  # the PAT

backend_type = "DATABRICKS"     # "AZURE_KEYVAULT" if integrating with Key Vault


# COMMAND ----------


scope = secret_scope_name          
secret_key = secret_key_name        
secret_value = connection_json      
# all of the values are mentioned in above cells ; 

# url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create" #first create and then put ; 
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"
#api for creating the secret scope ; 

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}


response = requests.post(url, headers=headers, data=json.dumps(payload))

if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

try:
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key=secret_key_name
    )
    
    print("Secret retrieved successfully.")
    
    parsed = json.loads(retrieved_json)
    print("Parsed JSON:")
    print(parsed)
    
except Exception as e:
    print("Secret verification failed:")
    print(e)

# we can either set up the secret scope of databricks either through databricks CLI or use REST API ; 

# COMMAND ----------

import requests
import json

scope = secret_scope_name          # Already existing scope
secret_key = 'gmail-notification'       # Name of the secret entry
secret_value = '#password' # Value to store securely

url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}



response = requests.post(url, headers=headers, data=json.dumps(payload))


if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)

# COMMAND ----------

try:
    retrieved_json = dbutils.secrets.get(
        scope="neobank-scope",
        key="gmail-notification"
    )
    
    print("Secret retrieved successfully.")
except Exception as e:
    print("Secret verification failed:")
    print(e)


# COMMAND ----------

