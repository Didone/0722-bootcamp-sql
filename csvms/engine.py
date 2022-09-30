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
        statement_history = []
        sql_data = sql.split(";")
        for sql_iter in sql_data:
            sql_str = sql_iter.strip().lower()
            self.commit = "commit" if sql_str == "commit" else ""
            sql_str = "" if self.commit == sql_str else sql_str
            if not self.commit:
                statement = sql_str.lower().split()[0]
                statement_history.append(statement)
            if sql_str != "":
                ast = parse(sql_str)
                name = ast[statement +
                           " table"] if statement == "create" else ast[statement]
            data = self._tables_data(name)
            if data is not None:
                data_list = data.replace("\n", ";").split(";")
                previous_pos = 0
                for i in range(1, len(data_list) + 1):
                    if i % 2 == 0:
                        inclusion.append(tuple(data_list[previous_pos:i]))
                        previous_pos = i
            """ sql_lista = [item.strip().lower()
                         for item in sql_iter if item.strip().lower() != ''] """
            if len(sql_str) > 0 or self.commit:
                #print("sql_str:", sql_str, "statement:", statement)
                if statement == 'insert':
                    ast = parse(sql_str)
                    value_extraction = ast['query']['select']
                    insert_name = ast['insert']
                    values = ()
                    for pos_value in range(len(value_extraction)):
                        if type(value_extraction[pos_value].get('value')) is dict:
                            values += (
                                list(value_extraction[pos_value].get('value').values())[0], )
                        else:
                            values += (
                                value_extraction[pos_value].get("value"), )
                    inclusion.append(values)
                    #print(name, inclusion)
                if self.commit:
                    try:
                        self._insert_into(
                            tbl_name=insert_name,
                            tbl_values=inclusion
                        )
                    except Exception as e:
                        print(e)
                    else:
                        print("Resgistros inseridos com sucesso!\n")
                if statement in ["create", "delete", "update"] or self.commit:
                    sql = sql_str
                    if sql != "":
                        ast = parse(sql)
                    if statement == 'create':
                        create_name = ast['create table']['name']
                        column = ast['create table']['columns']
                        try:
                            self._create_table(
                                tbl_name=create_name,
                                tbl_columns=column
                            )
                        except:
                            print("Erro. Tabela não foi criada.\n")
                        else:
                            print(
                                f"A Tabela {create_name} foi criada com sucesso!\n")
                    if statement == 'update':
                        set_column = list(ast['set'].keys())[0]
                        set_value = ast['set'][set_column]['literal']
                        where_value = ast['where']['eq'][1]['literal']
                        update_name = ast['update']
                        update_dict = {
                            'set_value': set_value, 'where_value': where_value}
                    if self.commit and 'update' in statement_history:
                        try:
                            self._update_table(
                                tbl_name=update_name,
                                update_data=update_dict
                            )
                        except:
                            print("Erro. Tabela não foi atualizada.\n")
                        else:
                            print(
                                f"A Tabela {update_name} foi atualizada com sucesso!\n")
                    if statement == 'delete':
                        where_value = ast['where']['eq'][1]['literal']
                        delete_name = ast['delete']
                    if self.commit and 'delete' in statement_history:
                        try:
                            self._delete_table(
                                tbl_name=delete_name,
                                delete_data=where_value
                            )
                        except:
                            print("Erro. Tabela não foi excluida.\n")
                        else:
                            print(
                                f"O registro solicitado foi excluido de {delete_name} com sucesso!\n")

    def sql_file_recovering(self, name: str):
        try:
            if os.path.exists(os.getcwd() + f"/{name}.sql"):

                with open(os.getcwd() + f"/{name}.sql") as f:
                    sql_file_data = f.read()
        except:
            print("Arquivo sql não encontrado.")
        else:
            print("Arquivo encontrado e processado.")
            file_str = self.sql_file_processing(sql_file_data)
            self.execute(file_str)

    def sql_file_processing(self, file_data: str) -> str:
        file_data = re.sub(r"--\s*([a-zA-Z0-9 ':(ãõç]+)", "",
                           file_data)
        file_lista = file_data.strip().replace("\n", "").replace("  ", "").split(";")
        file_lista.remove("")
        file_str = ";".join(file_lista)
        """ pattern = re.compile(r"--\s*([a-zA-Z0-9 ':(ãõç]+)")
            matches = pattern.finditer(file_data)
            fd = file_data.strip().split(';')
            for match in matches:
                print(match) """
        return file_str

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

    def _columns_data(self, tbl_name: str = ""):
        with open(os.getcwd() + "/data/catalog.json") as f:
            c_data = f.read()
        loaded_cdata = list(loads(c_data).values())
        if c_data is not None:
            cols = dict()
            for table in loaded_cdata:
                if table['name'] == f"default.{tbl_name}":
                    cnames = list(table['columns'].keys())
                    ctypes = list(table['columns'].values())
                    for name, type in zip(cnames, ctypes):
                        ctype = Table.dtypes[type]
                        cols[name] = ctype
        return cols

    def _tables_data(self, tbl_name):
        path = f"/data/default/{tbl_name}.csv"
        data = ""
        if os.path.exists(os.getcwd() + path):
            with open(os.getcwd() + path) as f:
                data = f.read()
        return data

    def _insert_into(self, tbl_name: str, tbl_values: list):
        cols = self._columns_data(tbl_name)
        #print(tbl_name, tbl_values)
        if (tbl_name and tbl_values) is not None:
            Table(name=tbl_name, columns=cols,
                  data=tbl_values).save()

    def _update_table(self, tbl_name: str, update_data: dict):
        cols = self._columns_data(tbl_name)
        tbl_data = self._tables_data(tbl_name)
        data_values = [value.split(';')
                       for value in tbl_data.split()]
        if (tbl_name and update_data) is not None:
            if len(tbl_data) > 0:
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
