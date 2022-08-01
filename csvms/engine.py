"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from csvms.table import Table
from mo_sql_parsing import parse


class Engine():
    """Class used to implement bootcamp tasks"""

    dtypes = {
        "string": str,
        "text": str,
        "int": int,
        "integer": int,
        "float": float,
        "boolean": bool}

    keywords = {
        "table": "create table",
        "insert": "insert into",
        "update": "update",
        "delete": "delete"
    }

    def execute(self, sql: str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        # TODO Implement your SQL engine

        if sql.lower().startswith(self.keywords['table']) or sql.lower().__contains__(self.keywords['table']):
            if not self._create_table(sql):
                raise Exception('Error creating table')
        else:
            raise NotImplementedError('SQL statement not implemented')

        # raise NotImplementedError

    def _create_table(self, rawSQL: str) -> bool:
        """Create table
        :param name: raw SQL statement"""

        # parse SQL statement
        parsed = parse(rawSQL)
        # get table name
        tableName = parsed.get('create table').get('name')
        # get table columns
        columns = parsed.get('create table').get('columns')

        # pasrse columns
        parsedColumns = dict()
        for x in columns:
            for _k_, v in x.items():
                if(type(v) == str):
                    key = v
                elif(type(v) == dict):
                    value = list(v.keys())[0]
            parsedColumns[key] = value

        # cast columns to correct type
        parsedColumns = self._type_cast(parsedColumns, self.dtypes)
        t = Table(name=tableName, columns=parsedColumns)
        if t.save():
            print(f'Table {tableName} created successfully')
            return True
        else:
            raise Exception(f'Error creating table {tableName}')

    def _type_cast(self, columns, dtype) -> dict:
        """Cast columns to correct type
        :param columns: columns to be casted
        :param dtype: dictionary with dtypes"""
        return {k: dtype[v] for k, v in columns.items()}
