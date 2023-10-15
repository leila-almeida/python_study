import unittest
import json_schema_to_hive as json_schema


class test_generate_columns_from_json(unittest.TestCase):
    def test_result(self):
        schema_properties = json_schema.get_schema("./schema.json")
        query = "CREATE EXTERNAL TABLE IF NOT EXIST TESTE (eid string,documentNumber string,name string,age int,address STRUCT < street string,number int,mailAddress boolean>) LOCATION 's3://iti-query-results/'"
        self.assertEqual(query, json_schema.generate_columns_from_json(schema_properties))


if __name__ == '__main__':
    unittest.main()
