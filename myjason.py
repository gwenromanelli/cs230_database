import json

x = {"DB_HOST":"county-instance-1.c70eoas2ow9v.us-east-2.rds.amazonaws.com",
"DB_NAME":"losangeles",
"DB_USER":"postgres",
"DB_PASSWORD":"cs230postgres",
"DB_PORT": 5432}

with open("database_variables.json",mode="w",encoding="utf-8") as write_file:
    json.dump(x,write_file,indent = 4, separators=(',',': '))