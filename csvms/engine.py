"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""

from csvms.table import Table
from mo_sql_parsing import parse

class Engine():
    """Class used to implement bootcamp tasks"""

    def execute(self, sql:str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        ast = parse(sql)
        if ast.get('create table') is not None:
            return self._create_table(
                tbl_name=ast['create table']['name'],
                tbl_columns=ast['create table']['columns']
                )
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
