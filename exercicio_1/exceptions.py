
class error_missing_mandatory_column(Exception):
        def __init__(self, message, value):
            message = f"Faltando campos obrigatórios no evento: {value}"
            super().__init__(message)


class error_more_columns_than_should(Exception):
    def __init__(self, message, value):
        message = f"Evento com mais campos do que esperado, esses campos não deveriam estar no evento: {value}"
        super().__init__(message)

class error_data_type_column(Exception):
        def __init__(self, message, value):
            message = f"Evento com tipo de dados incompativel, coluna  {value} não esta com o tipo esperado"
            super().__init__(message)
