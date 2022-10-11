import json
import boto3
from json2hive.utils import infer_schema
from json2hive.generators import generate_json_table_statement

_ATHENA_CLIENT = None

def create_hive_table_with_athena(query):
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def handler():
    
    f = open('C:/Users/Host/Documents/git/data-challenge/data-challenge/desafios/exercicio2/schema.json')
    schema_template = json.load(f)
    athena_schema = ''

    for field in schema_template['properties']:
        field_name = field  #recebe nome dos atributos
        field_type = schema_template['properties'][field]['type'] #recebe tipo dos atributos

        athena_schema += f'{field_name} {field_type},\n' 
    
    create_hive_table_with_athena = (f'CREATE EXTERNAL TABLE IF NOT EXISTS my_database_name.my_table_name({athena_schema})')
            
    
        

