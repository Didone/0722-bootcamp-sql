"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from mo_sql_parsing import parse
from csvms.table import Table

class Engine():
    """Class used to implement bootcamp tasks"""

    def execute(self, sql:str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        tokens = parse(sql)
        if tokens.get('create table') is not None:
            self._create_table(
                tbl_name=tokens['create table']['name'],
                tbl_columns=tokens['create table']['columns']
            )
        elif tokens.get('drop') is not None:
            self._drop_table(tokens['drop']['table'])
        else:
            raise NotImplementedError

    def _create_table(self, tbl_name:str, tbl_columns:list):
        cols = dict()
        for c in tbl_columns:
            cname = c['name']
            ctype = Table.dtypes[list(c['type'].keys())[0]]
            cols[cname]=ctype

        Table(
            name=tbl_name,
            columns=cols
        ).save()

    def _drop_table(self, name:str):
        print()
