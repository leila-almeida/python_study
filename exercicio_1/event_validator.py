import json
import boto3
import logging
import exercicio_1.exceptions as exceptions

_SQS_CLIENT = None




def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''

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
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''

    try:
        #Lendo o schema do evento para validações
        logging.debug("Iniciando leitura do schema do evento")
        event_schema = get_type(event)
        #Lendo o schema esperado
        logging.debug("Iniciando leitura do schema esperado")
        schema_properties = get_schema("exercicio_1/schema.json")
        logging.info("Schema do evento", payload=event_schema)
        logging.info("Schema esperado", payload=schema_properties)
        #Validações de campos e tipo de dados, caso tenha erro irá chamar uma exceção
        logging.debug("Iniciando validações do schema")
        validate_data_type(schema_properties, event_schema)
        #Enviando o evento para fila de eventos validos
        logging.debug("Enviando evento para fila de eventos validos")
        send_event_to_queue(event, "valid-events-queue")

    except ValueError as e:
        logging.error("Não é um json válido " + str(e))
        print("Não é um json válido " + str(e))
    except Exception as e:
        logging.error(f"Erro : " + str(e))
        print(e)




def validate_fields_mandatory_schema(event_schema, schema_properties):

    '''
    Função responsável por verificar se todos os campos obrigatórios do schema estão no evento
    Caso esteja faltando algum campo irá chamar uma exceção
    :param event_schema: Schema do evento enviado
    :param schema_properties: Schema esperado para evento
    '''
    for mandatory_column in schema_properties:
        #Verifica se as colunas obrigatórias estão no evento
        if mandatory_column not in event_schema.keys():
            logging.error(f"Faltando campos obrigatórios no evento: {mandatory_column}")
            raise exceptions.error_missing_mandatory_column(message="", value=mandatory_column)




def validate_fields_event(event_schema, schema_properties):

    '''
    Função responsável por verificar se o evento tem algum campo a mais do que no schema esperado
    Caso exista algum campo a mais irá chamar uma exceção
    :param event_schema: Schema do evento enviado
    :param schema_properties: Schema esperado para evento
    '''

    #verifica se tem algum campo no schema do evento que não esteja no schema esperado
    diff_event = set(event_schema.keys()).difference(schema_properties.keys())

    if len(diff_event) > 0:
        logging.error(f"Evento com mais campos do que esperado, esses campos não deveriam estar no evento: {diff_event}")
        raise exceptions.error_more_columns_than_should(message="", value=diff_event)



def validate_data_type(schema_properties,event_schema):
    '''
    Função responsável por verificar se o tipo de dados que veio no evento é o que esperamos caso não seja irá chamar uma exceção

    :param schema_properties: Schema esperado para evento
    :param event_schema: Schema do evento enviado
    '''

    #Verifica se o evento possui todos os campos obrigatórios
    validate_fields_mandatory_schema(event_schema, schema_properties['required'])
    #acessa as propriedas do schema esperado para pegar os campos e os tipos de dados
    schema_properties = schema_properties['properties']
    #Verifica se o evento tem mais campos do que esperamos
    validate_fields_event(event_schema, schema_properties)
    #loop pelo schema esperado
    for t in schema_properties:
        schema_type = schema_properties[t]['type']
        #Recursão para casos de jsons dentro do json (objetos)
        if schema_type == 'object':
            return validate_data_type(schema_properties[t], event_schema[t])
        else:
            #Validando se o tipo de dados esperado é o mesmo que está vindo no evento, caso seja diferente chama uma exceção
            if schema_type == 'string':
                if 'str' not in str(event_schema[t]):
                    logging.error(f"Evento com tipo de dados incompativel, {str(t)} deveria ser {schema_type}")
                    raise exceptions.error_data_type_column(message="", value=str(t))
            elif schema_type == 'integer':
                if 'int' not in str(event_schema[t]):
                    logging.error(f"Evento com tipo de dados incompativel, {str(t)} deveria ser {schema_type}")
                    raise exceptions.error_data_type_column(message="", value=str(t))
            elif schema_type == 'boolean':
                if 'bool' not in str(event_schema[t]):
                    logging.error(f"Evento com tipo de dados incompativel, {str(t)} deveria ser {schema_type}")
                    raise exceptions.error_data_type_column(message="", value=str(t))

def get_type(event):
    '''
    Função responsável por retornar o tipo dos dados do evento por campo
    :param event: Evento recebido que precisa ser validado
    :return: Retorna um dicionario com os campos e seus tipos de dados
    '''

    #Caso seja um dicionario ele navega campo a campo
    if isinstance(event, dict):
        dict_event = {key: get_type(event[key]) for key in event }

        return dict_event
    else:

        return str(type(event))



def get_schema(file_path):
    '''
    Função responsável por ler um json com o schema esperado
    :return: Retorna um dicionário com o schema esperado
    '''
    with open(file_path) as data_file:
        doc = json.loads(data_file.read())
        return doc

