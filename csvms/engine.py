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
        elif sql.lower().startswith(self.keywords['insert']) or sql.lower().__contains__(self.keywords['insert']):
            if not self._insert_into(sql):
                raise Exception('Error inserting into table')
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

    def _insert_into(self, rawSQL: str) -> bool:
        """Insert into table
        :param rawSQL: raw SQL statement"""
        # parse SQL statement

        # preprocess raw SQL statement
        listInserts = self._preprocess_rawsql(rawSQL)
        print(listInserts)

        commit = False

        # verify if there is a commit
        if('COMMIT' in listInserts):
            listInserts.remove('COMMIT')
            commit = True

        # parse each insert statement
        parsedInserts = []
        for insert in listInserts:
            parsedInserts.append(parse(insert))

        # create a dictionary with table name and your list of data to be inserted
        jobs = {}
        for statement in parsedInserts:
            valuesInsert = []
            for value in statement['query']['select']:
                if type(value['value']) == dict:
                    value = next(enumerate(value['value'].values()))[1]
                else:
                    value = value['value']
                valuesInsert.append(value)
            try:
                jobs[statement['insert']].append(valuesInsert)
            except KeyError:
                jobs[statement['insert']] = [valuesInsert]

        # insert data into table and check if it was successful
        control = []
        for key, lista in jobs.items():
            table = Table(name=key)
            for values in lista:
                table.append(*values)

            # commit if it was requested
            if commit:
                control.append(table.save())

        # check if all inserts were successful
        if False not in control:
            print('Inserts successful')
            return True
        else:
            raise Exception('Error inserting into table')

    def _preprocess_rawsql(self, rawSQL: str) -> list:
        """Preprocess raw SQL statement
        :param rawSQL: raw SQL statement"""
        raw_sql = list(map(str.strip, rawSQL.split('\n')))
        for x in raw_sql:
            if x.__contains__('--'):
                raw_sql.remove(x)
        raw_sql = ' '.join(raw_sql)
        return [x for x in list(
            map(str.strip, raw_sql.split(';'))) if x != '']
