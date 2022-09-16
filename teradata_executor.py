import teradatasql
import json
import os


class TeradataExecutor:
    def __init__(self, host, user, password):
        connection_string = f'{{"host":"{host}", "user":"{user}", "password":"{password}"}}'
        self.connection: teradatasql.TeradataConnection = TeradataExecutor.get_connection(connection_string)

    def __del__(self):
        self.connection.close()

    @staticmethod
    def get_connection(connection_string) -> teradatasql.connect:
        """
        Connects to teradata
        :param connection_string: Connection parameters
        :return: Connection object
        """
        return teradatasql.connect(connection_string, fake_result_sets=True, encryptdata=True)

    def execute_query_fetch_result_with_column_names(self, query) -> dict:
        """
        Retrieves data with column names from Teradata in a form of a dictionary
        :param query: Query
        :return: Dictionary representation of the result set from database
        """
        cursor = self.connection.cursor()
        cursor.execute(query)

        # Fetch metadata
        metadata = cursor.fetchall()
        cursor.nextset()

        result_rows = cursor.fetchall()
        if not result_rows:
            return {}

        cursor_description = cursor.description
        result = {}
        for row in result_rows:
            for index, field_value in enumerate(row):
                # Parse "fake_result_set" to get real column type and length
                column_info = json.loads(metadata[0][7])[index]
                column_type = column_info.get('TypeName')
                column_name = cursor_description[index][0]

                # RStrip values of type CHAR, DB automatically suffixes spaces to record on insert
                if column_type == "CHAR":
                    field_value = str(field_value).rstrip()

                field_values = result.get(column_name, [])
                field_values.append(field_value)
                result.update({column_name: field_values})

        return result

    def execute_query(self, query: str) -> bool:
        """
        Executes query of teradata warehouse
        :param query: Query
        :return: True or False whether execute succeeds or not
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        _ = cursor.fetchall()

        # Skip metadata
        cursor.nextset()

        result_rows = cursor.fetchall()
        return result_rows

    def dump_data_to_resultset(self, query: str) -> list:
        """
        Retrieves data from query
        :param query: Query text
        """
        result = []
        cursor = self.connection.cursor()
        cursor.execute(f"{query}")

        # Skip metadata
        cursor.nextset()

        # Fetch data into resultset
        result_rows = cursor.fetchall()
        if not result_rows:
            return []

        # Retrieves data
        for row in result_rows:
            temp_row = [str(r).rstrip() if r else '' for r in row]
            result.append(temp_row)
        return result

    def dump_data_to_file(self, query: str, filepath: str):
        """
        Dumps results of the query into given filepath
        :param query: Query
        :param filepath: Filepath for data dump
        """
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)

            # Skip metadata
            cursor.nextset()

            result_rows = cursor.fetchall()
            if not result_rows:
                return {}

            cursor_description = cursor.description
            first_row = result_rows.pop()

            columns = []
            for index, field_value in enumerate(first_row):
                column_name = cursor_description[index][0]
                columns.append(column_name)

            try:
                os.remove(filepath)
            except OSError:
                pass

            with open(filepath, 'a') as f:
                f.write(';'.join(columns) + '\n')
                f.write(';'.join(str(fr) if fr else '' for fr in first_row) + '\n')
                for row in result_rows:
                    f.write(';'.join(str(r) if r else '' for r in row) + '\n')
        finally:
            cursor.close()
