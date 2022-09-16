from typing import Dict, List, Tuple, Union
from cx_Oracle import DatabaseError
import cx_Oracle


class OracleExecutor:
    """
        Oracle config - plik konfiguracyjny połączenie (z builder/resources)
    """
    connection: cx_Oracle.Connection

    def __init__(self, config):
        self.oracle_config = config.get('ORACLE')
        self.is_error = False
        self.connect('DWH_MUP')

    def connect(self, area):
        self.connection = cx_Oracle.connect(self.oracle_config.get(area).get('USER'),
                                            self.oracle_config.get(area).get('PASSWORD'),
                                            self.oracle_config.get('HOST'))

    def execute_query(self, query, parameters: Union[List[object], Dict[str, object]] = None) -> bool:
        try:
            cursor: cx_Oracle.Cursor = self.connection.cursor()
            if parameters:
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            self.connection.commit()
            return True
        except DatabaseError as e:
            raise DatabaseError(f"Wystąpił błąd podczas wykonywania zapytania {query}\n{str(e)}")

    def execute_query_fetch_all(self, query) -> List[Tuple]:
        try:
            cursor: cx_Oracle.Cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            return cursor.fetchall()
        except DatabaseError as e:
            raise DatabaseError(f"Wystąpił błąd podczas wykonywania zapytania {query}\n{str(e)}")

    def call_procedure(self, procedure, args: []):
        cursor = self.connection.cursor()
        cursor.callproc(procedure, args)
        self.connection.commit()
