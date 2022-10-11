import json
import boto3

_SQS_CLIENT = None

def send_event_to_queue(event, queue_name):

    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")


def handler(event):

    f = open('C:/Users/Host/Documents/git/data-challenge/data-challenge/desafios/exercicio1/schema.json')
    schema_template = json.load(f)
    
    validate_non_object_type_fields(event, schema_template) #valida campos dos tipos comuns(int, string..)
    validate_address_field(event, schema_template) #valida campo do tipo objeto

    #Após validacao, eventos sao enviados para a fila
    send_event_to_queue(event, 'valid-events-queue')


def validate_non_object_type_fields(event, schema_template): #Funcao para todos os campos que nao sao do tipo "objeto"
    required_fields = sorted(schema_template['required'])
    event_fields = sorted([key for key in event])

    if event_fields != required_fields:
        raise Exception("Event schema doesn't match with required schema!") 
    
    for field in schema_template['properties']:
        if field != 'address':
            event_field = event[field]
            schema_template_field = schema_template['properties'][field]['examples'][0]

            if type(event_field) != type(schema_template_field):
                raise Exception(f"{field} has type {type(event_field)} where should be {type(schema_template_field)}") 


def validate_address_field(event, schema_template): #Funcao para tratar o campo do tipo objeto

    for field in schema_template['properties']:
        if field == 'address':
            event_field = event[field]
            address_required_fields = sorted(schema_template['properties'][field]['required'])
            address_event_fields = sorted([key for key in event_field])

            if address_required_fields != address_event_fields:
                raise Exception("Address event schema doesn't match with address required schema!") 

            for field in event_field:
                address_field = event_field[field]
                address_template_field = schema_template['properties']['address']['properties'][field]['examples'][0]
            
                if type(address_field) != type(address_template_field):
                    raise Exception(f"{address_field} has type {type(address_field)} where should be {type(address_template_field)}") 


            
