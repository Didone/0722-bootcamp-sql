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
        elif sql.lower().startswith(self.keywords['update']) or sql.lower().__contains__(self.keywords['update']):
            if not self._update(sql):
                raise Exception('Error updating table')
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

    def _update(self, rawSQL: str) -> bool:
        """Update table
        :param rawSQL: raw SQL statement"""

        listUpdate = self._preprocess_rawsql(rawSQL)
        commit = False
        if ('COMMIT' in listUpdate):
            listUpdate.remove('COMMIT')
            commit = True

        parsedUpdate = parse(listUpdate[0])
        parsedDict = {}
        for idx in parsedUpdate:
            if idx == 'set':
                fields = parsedUpdate[idx]
                for key in fields:
                    if type(fields[key]) == dict:
                        fields[key] = next(enumerate(fields[key].values()))[1]
                    parsedDict['fields'] = {key: fields[key]}
            elif idx == 'where':
                criteria = parsedUpdate[idx]
                for key in criteria:
                    if type(criteria[key]) == dict:
                        criteria[key] = next(
                            enumerate(criteria[key].values()))[1]
                    elif(type(criteria[key]) == list):
                        if(type(criteria[key][1]) == dict):
                            criteria[key][1] = next(
                                enumerate(criteria[key][1].values()))[1]

                    parsedDict['criteria'] = {
                        criteria[key][0]: criteria[key][1]}
                    parsedDict['op'] = key

            elif idx == 'update':
                parsedDict['table'] = parsedUpdate[idx]

        table = Table(name=parsedDict['table'])

        for idx in range(len(table)):
            fieldCriteria = next(enumerate(parsedDict['criteria'].keys()))[1]
            valueCriteria = next(enumerate(parsedDict['criteria'].values()))[1]
            fieldUpdate = next(enumerate(parsedDict['fields'].keys()))[1]
            valueUpdate = next(enumerate(parsedDict['fields'].values()))[1]
            if table[idx][fieldCriteria] == valueCriteria:
                row = table[idx]
                row[fieldUpdate] = valueUpdate
                table[idx] = tuple(row.values())

        if commit:
            if table.save():
                print('Update successful')
                return True
            else:
                raise Exception('Error updating table')

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
