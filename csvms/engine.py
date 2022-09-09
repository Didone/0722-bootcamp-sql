"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""

import os
import re
from mo_sql_parsing import parse
from csvms.table import Table
from json import loads


class Engine():
    """Class used to implement bootcamp tasks"""

    def __init__(self):
        self.commit = ""

    def execute(self, sql: str):
        """Execute SQL statement
        :param sql: String with sql statement"""
        # TODO Implement your SQL engine
        inclusion = []
        sql_lista = sql.split(";")
        print(sql_lista)
        sql_lista = [item.strip().lower()
                     for item in sql_lista if item.strip().lower() != '']
        self.commit = "commit" if sql_lista[-1] == "commit" else ""
        sql_lista.remove(
            self.commit) if "commit" in sql_lista else ""
        statement = sql.lower().split()[0]
        if statement == 'insert':
            for sql_insert in sql_lista:
                ast = parse(sql_insert)
                value_extraction = ast['query']['select']
                name = ast['insert']
                values = ()
                for pos_value in range(len(value_extraction)):
                    if type(value_extraction[pos_value].get('value')) is dict:
                        values += (
                            list(value_extraction[pos_value].get('value').values())[0], )
                    else:
                        values += (value_extraction[pos_value].get("value"), )
                inclusion.append(values)
            try:
                self._insert_into(
                    tbl_name=name,
                    tbl_values=inclusion
                )
            except:
                print("Erro. Tabela não informada|registro já inserido|faltando.\n")
            else:
                print("Resgistros inseridos com sucesso!\n")
        else:
            sql = ' '.join(sql_lista)
            ast = parse(sql)
            if statement == 'create':
                name = ast['create table']['name']
                column = ast['create table']['columns']
                try:
                    self._create_table(
                        tbl_name=name,
                        tbl_columns=column
                    )
                except:
                    print("Erro. Tabela não foi criada.\n")
                else:
                    print(f"A Tabela {name} foi criada com sucesso!\n")
            if statement == 'update':
                set_column = list(ast['set'].keys())[0]
                set_value = ast['set'][set_column]['literal']
                where_value = ast['where']['eq'][1]['literal']
                name = ast['update']
                update_dict = {
                    'set_value': set_value, 'where_value': where_value}
                try:
                    self._update_table(
                        tbl_name=name,
                        update_data=update_dict
                    )
                except:
                    print("Erro. Tabela não foi atualizada.\n")
                else:
                    print(f"A Tabela {name} foi atualizada com sucesso!\n")
            if statement == 'delete':
                where_value = ast['where']['eq'][1]['literal']
                name = ast['delete']
                try:
                    self._delete_table(
                        tbl_name=name,
                        delete_data=where_value
                    )
                except:
                    print("Erro. Tabela não foi excluida.\n")
                else:
                    print(
                        f"O registro solicitado foi excluido de {name} com sucesso!\n")

    def sql_file_recovering(self, name: str):
        try:
            if os.path.exists(os.getcwd() + f"/{name}.sql"):

                with open(os.getcwd() + f"/{name}.sql") as f:
                    sql_file_data = f.read()
        except:
            print("Arquivo sql não encontrado.")
        else:
            print("Arquivo encontrado.")
            return self.sql_file_processing(sql_file_data)

    def sql_file_processing(self, file_data: str):
        file_data = re.sub(r"--\s*([a-zA-Z0-9 ':(ãõç]+)", "",
                           file_data)
        fd = file_data.strip().replace("\n", "").replace("  ", "").split(";")
        fd.remove("")
        """ pattern = re.compile(r"--\s*([a-zA-Z0-9 ':(ãõç]+)")
        matches = pattern.finditer(file_data)
        fd = file_data.strip().split(';')
        for match in matches:
            print(match) """
        return fd

    def _create_table(self, tbl_name: str, tbl_columns: list):
        if (tbl_name and tbl_columns) is not None:
            cols = dict()
            for _c_ in tbl_columns:
                cname = _c_['name']
                ctype = Table.dtypes[list(_c_['type'].keys())[0]]
                cols[cname] = ctype
            Table(name=tbl_name, columns=cols).save()
        else:
            raise NotImplementedError

    def _data_retrieve(self, tbl_name: str):
        with open(os.getcwd() + "/data/catalog.json") as f:
            c_data = f.read()
        loaded_cdata = list(loads(c_data).values())
        with open(os.getcwd() + "/data/default/lista_frutas.csv") as f:
            lf_data = f.read()
        with open(os.getcwd() + "/data/default/tipo_frutas.csv") as f:
            tf_data = f.read()
        if c_data is not None:
            cols = dict()
            for table in loaded_cdata:
                if table['name'] == f"default.{tbl_name}":
                    cnames = list(table['columns'].keys())
                    ctypes = list(table['columns'].values())
                    for name, type in zip(cnames, ctypes):
                        ctype = Table.dtypes[type]
                        cols[name] = ctype
        return cols, lf_data, tf_data

    def _insert_into(self, tbl_name: str, tbl_values: list):
        cols, lf_data, tf_data = self._data_retrieve(tbl_name)
        current_table_data = lf_data if tbl_name == "lista_frutas" else tf_data
        if (tbl_name and tbl_values) is not None:
            if len(current_table_data) == 0 and self.commit:
                Table(name=tbl_name, columns=cols,
                      data=tbl_values).save()

    def _update_table(self, tbl_name: str, update_data: dict):
        cols, lf_data, tf_data = self._data_retrieve(tbl_name)
        current_table_data = lf_data if tbl_name == "lista_frutas" else tf_data
        data_values = [value.split(';')
                       for value in current_table_data.split()]
        if (tbl_name and update_data) is not None:
            if len(current_table_data) > 0 and self.commit:
                for value in data_values:
                    # value[0] -> nm_fruta; value[1] -> tp_fruta
                    if update_data['where_value'] == value[0]:
                        value[1] = update_data['set_value']
                    elif update_data['where_value'] == value[1]:
                        value[0] = update_data['set_value']
                tbl_values = [tuple(value) for value in data_values]
                Table(name=tbl_name, columns=cols,
                      data=tbl_values).save()
            else:
                raise NotImplementedError

    def _delete_table(self, tbl_name: str, delete_data: str):
        cols, lf_data, tf_data = self._data_retrieve(tbl_name)
        current_table_data = lf_data if tbl_name == "lista_frutas" else tf_data
        data_values = [value.split(';')
                       for value in current_table_data.split()]
        if (tbl_name and delete_data) is not None:
            if len(current_table_data) > 0 and self.commit:
                for value in data_values:
                    # value[0] -> nm_fruta; value[1] -> tp_fruta
                    if delete_data in value:
                        del data_values[data_values.index(value)]
                tbl_values = [tuple(value) for value in data_values]
                Table(name=tbl_name, columns=cols,
                      data=tbl_values).save()
            else:
                raise NotImplementedError
