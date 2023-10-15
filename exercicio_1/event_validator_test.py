import unittest
from exercicio_1.event_validator import validate_fields_mandatory_schema,validate_fields_event,validate_data_type,get_schema, get_type
import exercicio_1.exceptions as exceptions


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.schema_esperado = get_schema("./schema.json")

    # testando cenário com todas as informações necessárias
    event = {
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": 1,
            "mailAddress": True
        }}

    # testando cenário com faltando campo obrigatório no schema
    event_missing = {
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": 1,
            "mailAddress": True
        }}

    # testando cenário com campos a mais do que esperado no schema
    event_plus = {
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "teste":"teste",
        "address": {
            "street": "St. Blue",
            "number": 1,
            "mailAddress": True
        }}

    # testando cenário com tipo de dado diferente do esperado
    event_data_type = {
        "eid": "3e628a05-7a4a-4bf3-8770-084c11601a12",
        "documentNumber": "42323235600",
        "name": "Joseph",
        "age": 32,
        "address": {
            "street": "St. Blue",
            "number": 1,
            "mailAddress": 1
        }}


    def test_data_type_event(self):
        '''
        Validando chamada de exceção ao ter um tipo de dado errado no evento
        '''
        event_schema = get_type(self.event_data_type)
        with self.assertRaises(exceptions.error_data_type_column):
                validate_data_type(self.schema_esperado, event_schema)


    def test_data_type_event_happy_flow(self):
        '''
        Validando que ao schema do evento estar certo  não haverá chamada de exceção, sem retorno
        '''

        event_schema = get_type(self.event)

        self.assertIsNone(validate_data_type(self.schema_esperado, event_schema))

    def test_mandatory_fields_event(self):
        '''
        Validando chamada de exceção ao ter um dado obrigatório faltando no evento
        '''
        event_schema = get_type(self.event_missing)
        with self.assertRaises(exceptions.error_missing_mandatory_column):
                validate_fields_mandatory_schema(event_schema,self.schema_esperado["required"])

    def test_mandatory_fields_event_happy_flow(self):
        '''
         Validando que ao ter todos os campos obrigatório no evento não haverá chamada de exceção, sem retorno
        '''

        event_schema = get_type(self.event)

        self.assertIsNone(validate_fields_mandatory_schema(event_schema, self.schema_esperado["required"]))

    def test_event_fields_event(self):
        '''
        Validando chamada de exceção ao mais campos do que esperado no schema
        '''
        event_schema = get_type(self.event_plus)
        with self.assertRaises(exceptions.error_more_columns_than_should):
                validate_fields_event(event_schema,self.schema_esperado["properties"])

    def test_event_fields_event_happy_flow(self):
        '''
         Validando que ao não ter  campos a mais do que esperado no evento não haverá chamada de exceção, sem retorno
        '''

        event_schema = get_type(self.event)

        self.assertIsNone(validate_fields_event(event_schema,self.schema_esperado["properties"]))

    def test_get_type_event(self):
        resulting_dictionary = {'eid': "<class 'str'>",
                                'documentNumber': "<class 'str'>",
                                'name': "<class 'str'>",
                                'age': "<class 'int'>",
                                'address': {'street': "<class 'str'>",
                                            'number': "<class 'int'>",
                                            'mailAddress': "<class 'bool'>"}
                                }
        self.assertDictEqual(resulting_dictionary,get_type(self.event))



if __name__ == '__main__':
    unittest.main()
