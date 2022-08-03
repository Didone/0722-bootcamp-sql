"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from mo_sql_parsing import parse
from csvms.table import Table

class Engine():
    """Class used to implement bootcamp tasks"""

    def __init__(self) -> None:
        self.modified_tbls = dict()    

    def execute(self, sql:str):
        """Execute SQL statement
        :param sql: String with sql statement"""

        commands = list()

        if ';' in sql:
            if sql[-1] == ';':
                commands = sql.split(';')[:-1]
            else:
                commands = sql.split(';')

        else:
            commands.append(sql)

        for command in commands:
            command = command.replace('\n', '')
            if command.replace(' ', '').upper() == 'COMMIT':
                self.save_tables()
            else:
                tokens = parse(command)
                if tokens.get('create table') is not None:
                    self._create_table(
                        tbl_name=tokens['create table']['name'],
                        tbl_columns=tokens['create table']['columns']
                    )
                elif tokens.get('drop') is not None:
                    self._drop_table(tokens['drop']['table'])
                elif tokens.get('insert') is not None:
                    self._insert(tbl_name=tokens['insert'], values=tokens['query']['select'])
                else:
                    raise NotImplementedError

    def _create_table(self, tbl_name:str, tbl_columns:list):
        cols = dict()
        for c in tbl_columns:
            cname = c['name']
            ctype = Table.dtypes[list(c['type'].keys())[0]]
            cols[cname]=ctype


        # self.modified_tbls[tbl_name] = (
        Table(
            name=tbl_name,
            columns=cols
        ).save()
        # )

    def _drop_table(self, name:str):
        print()
    
    def _insert(self, tbl_name:str, values:list):
        new_row = list()
        tbl = self.load_table(tbl_name)
        for v in values:
            if isinstance(v['value'], dict) :
                new_row.append(v['value']['literal'])
            else:
                new_row.append(v['value'])

        tbl.append(*new_row)
        self.modified_tbls[tbl_name] = tbl

    def save_tables(self):
        for tbl in self.modified_tbls.values():
            tbl.save()
            print('Tabela', tbl.name, 'foi salva no SCHEMA', tbl.database.name)
        self.modified_tbls.clear()

    def load_table(self, tbl_name:str) -> Table:
        if self.modified_tbls.get(tbl_name) is not None:
            return self.modified_tbls[tbl_name]
        else:
            return Table(tbl_name)


