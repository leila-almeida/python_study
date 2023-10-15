import json
import logging

_ATHENA_CLIENT = None


def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''

    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )


def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''
    try:
        # Lendo o schema esperado
        logging.debug("Iniciando leitura do schema esperado")
        schema_properties = get_schema("exercicio_2/schema.json")
        logging.info("Schema da tabela ", payload=schema_properties)
        logging.debug("Iniciando criação de query")
        query_create_table = generate_columns_from_json(schema_properties)
        logging.debug("Criando tabela no athena baseada na query montada")
        create_hive_table_with_athena(query_create_table)
    except Exception as e:
        logging.error(f"Erro : " + str(e))
        print(e)



def generate_columns_from_json(schema_properties):
    '''
    Função responsável por gerar query com o DDL baseado no schema.json

    :param schema_properties: Schema da tabela a ser criada

    :return: Retorna a query com o DDL pronto para ser executado
    '''
    # Váriavel responsável por montar a query para ser executada no athena
    create_table_athena = "CREATE EXTERNAL TABLE IF NOT EXIST TESTE ("

    schema_properties = schema_properties['properties']
    #loop pelo schema esperado
    for index, column in enumerate(schema_properties):
        schema_type = schema_properties[column]['type']
        #Recursão para casos de jsons dentro do json (objetos)
        if schema_type == 'object':
            #Necessário criação de struct quando tem objetos dentro do objeto
            create_table_athena += column + " STRUCT < "
            for index_object, column_object in enumerate(schema_properties[column]['properties']):
                schema_type_object = schema_properties[column]['properties'][column_object]['type']
                if len(schema_properties[column]['properties']) == (index_object + 1):

                    # caso seja a última coluna não pode ter vírgula e fechar o struct caso necessário
                    create_table_athena += str(column_object) + " " + str(schema_type_object) + ">)"
                    if len(schema_properties) < (index +1):
                        create_table_athena += ","
                else:
                    # se não for a última coluna necessário adicionar vírgula
                    if schema_type_object == 'integer':
                        schema_type_object = 'int'
                    create_table_athena += str(column_object) + " " + str(schema_type_object) + ","
        else:
            if len(schema_properties) == (index +1) :
                #caso seja a última coluna não pode ter vírgula e fechar o struct caso necessário
                create_table_athena += str(column) + " " + str(schema_type) + ">)"
            else:
                #se não for a última coluna necessário adicionar vírgula
                if schema_type == 'integer':
                    schema_type = 'int'

                create_table_athena += str(column) + " " + str(schema_type) + ","
    create_table_athena += " LOCATION 's3://iti-query-results/'"


    return create_table_athena




def get_schema(file_path):
    '''
    Função responsável por ler um json com o schema esperado
    :return: Retorna um dicionário com o schema esperado
    '''
    with open(file_path) as data_file:
        doc = json.loads(data_file.read())
        return doc
