"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from mo_sql_parsing import parse
from csvms.table import Table

class Engine():
    """Class used to implement bootcamp tasks"""

    def __init__(self) -> None:
        self.modified_tbls = dict()    

    def execute(self, sql:str, file=False):
        """Execute SQL statement
        :param sql: String with sql statement"""

        if file:
            f = open(sql, 'r+')
            sql = ''
            for l in f:
                print("antes:",l)
                sql += l[:l.find("--")]
                print("depois:",l)
            print(sql)

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
                self._save_tables()
            else:
                try:
                    tokens = parse(command)
                except:
                    continue

                if tokens.get('create table') is not None:
                    self._create_table(
                        tbl_name=tokens['create table']['name'],
                        tbl_columns=tokens['create table']['columns']
                    )
                elif tokens.get('insert') is not None:
                    self._insert(
                        tbl_name=tokens['insert'], 
                        values=tokens['query']['select']
                    )
                elif tokens.get('update') is not None:
                    self._update_or_delete(
                        tbl_name=tokens['update'], 
                        set=tokens['set'], 
                        where=tokens['where']
                    )
                elif tokens.get('delete') is not None:
                    self._update_or_delete(
                        tbl_name=tokens['delete'], 
                        where=tokens['where']
                    )
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

        print('entrou')

    def _drop_table(self, name:str):
        print()
    
    def _insert(self, tbl_name:str, values:list):
        new_row = list()
        tbl = self._load_table(tbl_name)
        for v in values:
            if isinstance(v['value'], dict) :
                new_row.append(v['value']['literal'])
            else:
                new_row.append(v['value'])

        tbl.append(*new_row)
        self.modified_tbls[tbl_name] = tbl

    def _update_or_delete(self, tbl_name:str, set:dict or None = None, where=dict):
        tbl = self._load_table(tbl_name)

        op_str = list(where.keys())[0]
        op = Table.operations[op_str]

        op_val = None
        op_col = None
        
        if set is not None:
            set_col_val = dict()

        where = where.get(op_str)

        if isinstance(where, list):
            op_col = where[0]
            if isinstance(where[1], dict):
                op_val = where[1].get('literal')
            else:
                op_val = where[1]
        
        if set is not None:
            for k, v in set.items():
                if isinstance(v, dict):
                    v = v['literal']
                set_col_val[k] = v

        for row in range(len(tbl)):
            rlist = tbl[row]
            if op(rlist[op_col], op_val):
                if set is not None:
                    for k, v in set_col_val.items():
                        rlist[k] = v
                    tbl[row] = tuple(rlist.values())
                else:
                    del tbl[row]
        self.modified_tbls[tbl_name] = tbl

    def _save_tables(self):
        for tbl in self.modified_tbls.values():
            tbl.save()
            print('Tabela', tbl.name, 'foi salva no SCHEMA', tbl.database.name)
        self.modified_tbls.clear()

    def _load_table(self, tbl_name:str) -> Table:
        if self.modified_tbls.get(tbl_name) is not None:
            return self.modified_tbls[tbl_name]
        else:
            return Table(tbl_name)

    def _column_index(self, tbl:Table, val:str) -> int:
        for num, _ in enumerate(tbl.columns.keys()):
            if _ == val:
                return num

