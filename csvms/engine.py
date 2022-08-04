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
