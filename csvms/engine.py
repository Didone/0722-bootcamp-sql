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
        self._transaction = dict()
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
        
        elif ast.get('select_distinct') is not None:
            tables = ast.get('from')
            columns = ast.get('select_distinct')
            condition = ast.get('where')

            table = self._select(
                tables_names=tables,
                columns_name=columns,
                select_condition = condition
            )

            return self._distinct(table, ast.get('select_distinct').get('name'))
            


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


    def _distinct(self, table:"Table", distinct_column:str):
        distinct_values = list()
        rows = list()
        for idx, row in enumerate(table):
            if row[0] not in distinct_values:
                distinct_values.append(row[0])
                rows.append(row)

        return Table(
            name = f"({table.name}.distinct)",
            columns={k:v for k,v in table.columns.items()},
            data=rows)
    

    def _join(self, tables_names:list) -> "Table":
        # Armazena a primeira tabela Tabela 
        table = Table(tables_names[0].get('value')).ρ(tables_names[0].get('name'))

        joins_operation = ['value', 'inner join', 'left join', 'right join']

        for i in range(1, len(tables_names)):
            # Aramazena a condição do join
            condition_join = tables_names[i].get('on')

            # Verifica qual tipo de join será realizado
            for join_type in joins_operation:
                if tables_names[i].get(join_type) is not None:
                    # Para caso de junção cruzada
                    if join_type == 'value':
                        right_table = Table(tables_names[i].get(join_type)).ρ(tables_names[i].get('name'))

                        table = (table*right_table)
                    # Para caso de joins
                    else:
                        # Verifica a existência de subquerys
                        if isinstance(tables_names[i].get(join_type).get('value'), dict):
                            sub_query = tables_names[i].get(join_type).get('value')
                            right_table = self._select(
                                tables_names = sub_query.get('from'),
                                columns_name = sub_query.get('select'),
                                select_condition = sub_query.get('where')
                                ).ρ(tables_names[i].get(join_type).get('name'))
                        else:
                            right_table = Table(tables_names[i].get(join_type).get('value')).ρ(tables_names[i].get(join_type).get('name'))

                        if join_type == 'inner join':
                            table = table.ᐅᐊ(right_table, condition_join)
                        
                        elif join_type == 'left join':
                            table = table.ᗌᐊ(right_table, condition_join)

                        elif join_type == 'right join':
                            table = table.ᐅᗏ(right_table, condition_join)

        return table


    def _extended_projection(self, columns_name:Union[list, str]):
        extended_projection = alias_extended_projection = None
        # verifica se as columnas são uma list
        if isinstance(columns_name, list):
            # Percorre todas as colunas da projeção
            for column in columns_name:
                # Verifica se é projeção estendida 
                if isinstance(column.get('value'), dict):
                    extended_projection = column.get('value')
                    alias_extended_projection = column.get('name')

        return extended_projection, alias_extended_projection


    def _select(self, tables_names:Union[list, str], columns_name:Union[list, str], select_condition:dict):
        # Verifica se há projeção estendida 
        extended_projection, alias_extended_projection = self._extended_projection(columns_name)

        # Verifica se há join
        if isinstance(tables_names, list):
            # Realiza a operação de junção e seleção
            table = self._join(tables_names)\
                                        .σ(select_condition)\
                                            .Π(extended_projection, alias_extended_projection)\
                                                .π(columns_name)

        # Caso for um SELECT sem join
        else:
            # Realiza a seleção
            table = Table(tables_names)\
                                .σ(select_condition)\
                                    .Π(extended_projection, alias_extended_projection)\
                                        .π(columns_name)

        return table
