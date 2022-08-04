"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""

from optparse import Values
from csvms.table import Table
from mo_sql_parsing import parse

class Engine():
    """Class used to implement bootcamp tasks"""

    def execute(self, sql:str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        _sql_split = sql.split(';')
        ast = parse(_sql_split[0])

        if ast.get('create table') is not None:
            return self._create_table(
                tbl_name=ast['create table']['name'],
                tbl_columns=ast['create table']['columns']
                )
        
        elif ast.get('insert') is not None:
            ast = sql.split(';')
            values = list()
            list_values = list()

            for i in range(0, len(ast)):
                if 'COMMIT' not in ast[i]:
                    insert = parse(ast[i])
                    insert = insert['query']['select']
                    values = [v['value']['literal'] for v in insert]
                    list_values.append(values)
                    values = list()
                else:
                    break
            
            ast = parse(_sql_split[0])
            tbl_insert = self._insert_values(
                tbl_name=ast.get('insert'),
                insert_values=list_values
            )
            ast = sql.split(';')
            if 'COMMIT' in ast: # Salva no banco se tiver o comando 'COMMIT' 
                tbl_insert.save()

        elif ast.get('update') is not None:
            where_values = dict()
            set_values = dict()

            set_v = ast['set']
            for k in set_v.keys():
                for v in set_v[k]:
                    set_values[k] = set_v[k][v]
                    
            where_v = ast['where']['eq']
            where_values[where_v[0]] = where_v[1]['literal']

            self._update(
                tbl_name=ast['update'],
                where_values=where_values,
                set_values=set_values
            ).save()

        elif ast.get('delete') is not None:
            where_values = dict()
                    
            where_v = ast['where']['eq']
            where_values[where_v[0]] = where_v[1]['literal']

            print('where values', where_values)

            self._delete(
                tbl_name=ast['delete'],
                where_values=where_values,
            ).save()

        else:
            raise NotImplementedError
    
    def _create_table(self, tbl_name:str, tbl_columns:list):
        ''' Cria uma tabela.'''
        cols = dict()
        for _c_ in tbl_columns:
            cname = _c_['name']
            ctype = Table.dtypes[list(_c_['type'].keys())[0]]
            cols[cname] = ctype

        Table(name=tbl_name, columns=cols).save()

        return f'A tabela {tbl_name} foi criada com sucesso!'

    def _insert_values(self, tbl_name:str, insert_values:list):
        ''' Inserir valores na tabela'''
        tbl = Table(tbl_name)

        for values in insert_values:
            tbl.append(*values)

        return tbl

    def _update(self, tbl_name:str, where_values:dict, set_values:dict):
        '''Update tabela'''
        tbl = Table(tbl_name)

        for idx in range(len(tbl)):
            if tbl[idx][list(where_values.keys())[0]] == list(where_values.values())[0]:
                row = tbl[idx]
                row[list(set_values.keys())[0]] = list(set_values.values())[0]
                tbl[idx] = tuple(row.values())

        print(tbl)
        return tbl

    def _delete(self, tbl_name:str, where_values:dict):
        '''Update tabela'''
        tbl = Table(tbl_name)

        for idx in reversed(range(len(tbl))):
            if tbl[idx][list(where_values.keys())[0]] == list(where_values.values())[0]:
                del tbl[idx]

        print(tbl)
        return tbl
