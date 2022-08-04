"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from cmath import exp
from mo_sql_parsing import parse
from csvms.table import Table
from re import sub
from typing import Union

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

        elif ast.get('update') is not None:
            self.nome_tabela = ast.get('update')
            set = ast.get('set')
            condition=ast.get('where')

            if self.tbl == None:
                self.tbl = Table(self.nome_tabela)
            
            return self._update(
                update_value=set,
                update_condicion=condition
            )

        elif ast.get('delete') is not None:
            self.nome_tabela = list(ast.values())[1]
            operation = list(ast['where'].keys())[0]

            if self.tbl == None:
                self.tbl = Table(self.nome_tabela)

            return self._delete(
                condicion=[
                    operation,                                      # operador
                    ast['where'][operation][0],                     # nome da coluna
                    list(ast['where'][operation][1].values())[0]    # valor
                ] 
            )
        
        elif ast.get('select') is not None:
            tables = ast.get('from')
            columns = ast.get('select')
            condition = ast.get('where')

            return self._select(
                tables_names=tables,
                columns_name=columns,
                select_condition = condition
            )


    def _multiple_statements(self, sql:str):
        if '\n' in sql:
            list_comands = sub('[\n]', ' ', sql).split(';')[:-1]
        else:
            list_comands = sql.split(';')[:-1]
        
        for execution in list_comands:
            if execution.upper().strip() == 'COMMIT':
                # Table(self.nome_tabela).save()
                self.tbl.save()
                print(f'COMMIT na tabela {self.nome_tabela} foi realizado com sucesso!')
                self.tbl = None
            elif execution.upper().strip() == 'ROLLBACK':
                self.tbl = None
                print(f'Descartadas todas as alterações desta transação')
            else:
                self.execute(execution.strip())


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
            try:
                values.append(_v_['value']['literal'])
            except TypeError:
                values.append(_v_['value'])
                
        self.tbl.append(*values)


    def _update(self,update_value:dict, update_condicion:dict):
        # Condicion where:
        operation = list(update_condicion.keys())[0]
        column_name_where = list(update_condicion.values())[0][0]
        try:
            value_condicion = list(update_condicion.values())[0][1].get('literal')
        except AttributeError:
            value_condicion = list(update_condicion.values())[0][1]

        # lista com nomes da columnas que possuem valores para substituir
        columns_name_subs = list(update_value.keys())

        # Itera sobre a tabela para percorrer todas as sua linha
        for idx in range(len(self.tbl)):
            # em busca daquelas que atendem às condições da clausula WHERE
            if Table.operations[operation](self.tbl[idx][column_name_where], value_condicion):
                # Armazena o conteudo da linha
                row = self.tbl[idx]
                # For para substituir os valores (caso exista mais do que um valor para ser substituido)
                for name in (columns_name_subs):
                    # Altera o valor da linha selecionado para cada coluna
                    try:
                        row[name] = list(update_value.get(name).values())[0]
                    except AttributeError:
                        row[name] = update_value.get(name)
                
                # Atualiza a linha, já como valor novo, na tabela
                self.tbl[idx] = tuple(row.values())



    def _delete(self, condicion:list):
        for idx in range(len(self.tbl)):
            if Table.operations[condicion[0]](self.tbl[idx][condicion[1]], condicion[2]):
                del self.tbl[idx]

    def _select(self, tables_names:Union[list, str], columns_name:list, select_condition:dict):
        if type(tables_names) == list:
            left_table = Table(tables_names[0].get('value')).ρ(tables_names[0].get('name'))
            condition_join = tables_names[1].get('on')

            if tables_names[1].get('inner join') is not None:
                right_table = Table(tables_names[1].get('inner join').get('value')).ρ(tables_names[1].get('inner join').get('name'))
                if select_condition == None:
                    return (left_table.ᐅᐊ(right_table, condition_join)).π(columns_name)
                else:
                    return (left_table.ᐅᐊ(right_table, condition_join)).σ(select_condition).π(columns_name)

            elif tables_names[1].get('right join') is not None:
                right_table = Table(tables_names[1].get('right join').get('value')).ρ(tables_names[1].get('right join').get('name'))
                if select_condition == None:
                    return (left_table.ᐅᗏ(right_table, condition_join)).π(columns_name)
                else:
                    return (left_table.ᐅᗏ(right_table, condition_join)).σ(select_condition).π(columns_name)

            else:
                # CROSS JOIN:
                left_table = Table(tables_names[0].get('value')).ρ(tables_names[0].get('name'))
                right_table = Table(tables_names[1].get('value')).ρ(tables_names[1].get('name'))

                return (left_table*right_table).σ(select_condition).π(columns_name)

        else:
            table = Table(tables_names)
            if select_condition == None:
                return table.π(columns_name)
            else:
                return table.π(columns_name).σ(select_condition)

