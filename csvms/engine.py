"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from mo_sql_parsing import parse
from csvms.table import Table
from re import sub

class Engine():
    """Class used to implement bootcamp tasks"""

    def __init__(self) -> None:
        self.nome_tabela = None
        self.tbl = None
        

    def execute(self, sql:str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        # TODO Implement your SQL engine

        # Verifica a existencia de multiplas statements
        if ';' in sql:
            return self._multiple_statements(sql)
        else:
            ast = parse(sql)


        # Testando as operações
        if ast.get('create table') is not None:
            return self._create_table(
                tbl_name=ast['create table']['name'],
                tbl_columns=ast['create table']['columns']
            )

        elif ast.get('insert') is not None:
            self.nome_tabela = list(ast.values())[1]

            if self.tbl == None:
                self.tbl = Table(self.nome_tabela)

            return self._insert_into(
                tbl_name=ast['insert'],
                tbl_values=ast['query']['select']
            )

        else:
            print('Execução finalizada!')


    def _multiple_statements(self, sql:str):
        if '\n' in sql:
            list_comands = sub('[\n]', '', sql).split(';')[:-1]
        else:
            list_comands = sql.split(';')[:-1]
        
        for execution in list_comands:
            if execution.upper() == 'COMMIT':
                # Table(self.nome_tabela).save()
                self.tbl.save()
                return f'FOi realiizado o COMMIT na tabela {self.nome_tabela}'
            else:
                self.execute(execution)


    def _create_table(self, tbl_name:str, tbl_columns:list):
        # Criação do dicionário que será passado como parâmentro para columns
        cols = dict()
        for _c_ in tbl_columns:
            cnames = _c_['name']
            # Trasformando a str 'text' no tipo de dado str
            ctype = Table.dtypes[list(_c_['type'].keys())[0]]
            cols[cnames] = ctype

        Table(
            name=tbl_name,
            columns=cols
            ).save()
        return f'A Table {tbl_name} foi criada com sucesso!'

    def _insert_into(self, tbl_name:str, tbl_values:list):

        values = list()
        for _v_ in tbl_values:
            values.append(_v_['value']['literal'])

        self.tbl.append(*values)
