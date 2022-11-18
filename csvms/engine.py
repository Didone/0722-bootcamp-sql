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
        insert_name_list, update_name_list = [], []
        update_list_set_column, update_list_set_value, update_list_where_value = [], [], []
        delete_list_where_value, delete_name_list = [], []
        inserts = {}
        inclusion = []
        update = []
        statement_history = []
        sql_data = sql.split(";")
        sql_data.remove('') if '' in sql_data else sql_data
        for sql_iter in sql_data:
            sql_str = sql_iter.strip().lower()
            self.commit = "commit" if sql_str == "commit" else ""
            sql_str = "" if self.commit == sql_str else sql_str
            if not self.commit:
                statement = sql_str.lower().split()[0]
                statement_history.append(statement)
            if sql_str != "":
                ast = parse(sql_str)
                modifying_for_extraction = statement + \
                    ' table' if statement == "create" else statement
                tbl_name = ast[modifying_for_extraction]['name'] if statement == \
                    'create' else ast[modifying_for_extraction]
                data = self._tables_data(tbl_name)
            if data is not None:
                data_list = data.replace("\n", ";").split(";")
                previous_pos = 0
                for i in range(1, len(data_list) + 1):
                    if i % 2 == 0:
                        inclusion.append(tuple(data_list[previous_pos:i]))
                        previous_pos = i
            if len(sql_str) > 0 or self.commit:
                statement = "" if len(self.commit) > 0 else statement
                if statement == 'insert':
                    value_extraction = ast['query']['select']
                    insert_name = ast['insert']
                    insert_name_list.append(insert_name)
                    values = ()
                    for pos_value in range(len(value_extraction)):
                        if type(value_extraction[pos_value].get('value')) is dict:
                            values += (
                                list(value_extraction[pos_value].get('value').values())[0], )
                        else:
                            values += (
                                value_extraction[pos_value].get("value"), )
                    values = [values]
                    for value in values:
                        if len(inserts) > 0 and insert_name in inserts.keys():
                            inserts[insert_name] += [value]
                        else:
                            inserts[insert_name] = [value]
                # inclusion.append(values)
                # print(name, inclusion)
                if self.commit:
                    try:
                        self._insert_into(
                            tbls_names=list(set(insert_name_list)),
                            inserts=inserts
                        )
                    except Exception as e:
                        print(e)
                    else:
                        print("Resgistros inseridos com sucesso!\n")
                if statement in ["create", "delete", "update"] or self.commit:
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
                        # print(ast)
                        set_column = list(ast['set'].keys())[0]
                        if isinstance(ast['set'][set_column], dict):
                            set_value = ast['set'][set_column]['literal']
                        else:
                            set_value = ast['set'][set_column]

                        if isinstance(ast['where']['eq'][1], dict):
                            where_value = ast['where']['eq'][1]['literal']
                        else:
                            where_value = ast['where']['eq'][1]
                        update_list_set_column.append(set_column)
                        update_list_set_value.append(set_value)
                        update_list_where_value.append(where_value)
                        update_name = ast['update']
                        update_name_list.append(update_name)
                        update = {
                            "set_value": update_list_set_value, "set_column": update_list_set_column, "where_column": update_list_where_value}
                    if self.commit and 'update' in statement_history:
                        try:
                            self._update_table(
                                tbls_names=update_name_list,
                                updates=update
                            )
                        except:
                            print("Erro. Tabela não foi atualizada.\n")
                        else:
                            for update_name in update_name_list:
                                print(
                                    f"A Tabela {update_name} foi atualizada com sucesso!\n")
                    if statement == 'delete':
                        if 'where' in ast.keys():
                            where_value = ast['where']['eq'][1]['literal']
                            delete_name = ast['delete']
                        elif 'returning' in ast.keys():
                            where_value = ast['returning']['value']['eq'][1]['literal']
                            delete_name = ast['delete'][:-5]
                        delete_list_where_value.append(where_value)
                        delete_name_list.append(delete_name)
                    if self.commit and 'delete' in statement_history:
                        try:
                            self._delete_table(
                                tbls_names=delete_name_list,
                                delete_data=delete_list_where_value
                            )
                        except:
                            print("Erro. Tabela não foi excluida.\n")
                        else:
                            for delete_name in delete_name_list:
                                print(
                                    f"O registro solicitado foi excluido de {delete_name} com sucesso!\n")
                if statement == 'select':
                    select_query = ast['select']
                    from_table = ast['from']
                    if "where" in ast:
                        where_query = ast['where']
                    else:
                        where_query = []
                    try:
                        self.sql_query(
                            from_clause=from_table,
                            select=select_query,
                            where=where_query
                        )
                    except:
                        print("Erro. Query não executada.")
                    else:
                        print("Query executada com sucesso.")

    def sql_query(self, from_clause: list, select: list, where: list):
        tbls_name, cols_name, tbls_pos = [], [], []
        cols_list = []
        col_dict = {}
        default_table = {}

        if isinstance(from_clause, list):
            for value in select:
                for pos, tbl_name in enumerate(from_clause):
                    tbl_alias = tbl_name['name']
                    if value['value'][0] == tbl_alias:
                        tbls_name.append(tbl_name['value'])
                        tbls_pos.append(pos)
                        cols_name.append(value['value'])
        else:
            tbls_name = [from_clause]
        for name in tbls_name:
            cols = self._columns_data(name)
            cols_list.append(cols)
            tbl_data = self._tables_data(name)
            tbl_data_list = [value.split(';') for value in tbl_data.split()]
            for pos, col in enumerate(cols):
                for data in tbl_data_list:
                    if len(col_dict) > 0 and col in col_dict.keys():
                        col_dict[col] += [data[pos]]
                    else:
                        col_dict[col] = [data[pos]]
            default_table[name] = col_dict
            col_dict = {}
        if where:
            tbl_values_keys = []
            cols_values = []
            temp_set = set()
            values_tbl = default_table.values()
            rep_values, tbl_1, tbl_2, temp_col_val = [], [], [], []
            new_col_val, cols = {}, {}
            for value in values_tbl:
                tbl_values_keys.extend(value)
            inter_column = list(set(
                [x for x in tbl_values_keys if tbl_values_keys.count(x) > 1]))
            for values in values_tbl:
                cols_values.append(set(values[inter_column[0]]))
            for values in cols_values:
                if len(temp_set) > 0:
                    if len(values) > len(temp_set):
                        diff_value = list(values - temp_set)[0]
                    else:
                        diff_value = list(temp_set - values)[0]
                temp_set = values
            for key, values in default_table.items():
                if diff_value in values[inter_column[0]]:
                    remove_value_pos = values[inter_column[0]].index(
                        diff_value)
                    tbl_name = key
            for key, values in default_table.items():
                if key == tbl_name:
                    for value in list(values.values()):
                        del value[remove_value_pos]
            pos = 0
            while len(tbl_1) == 0 or len(tbl_2) == 0:
                for key, values in default_table.items():
                    if tbls_pos[pos] == max(tbls_pos) and tbls_name[pos] == key:
                        tbl_2.extend(values[inter_column[0]])
                        for k_values in values:
                            if k_values != inter_column[0]:
                                temp_col_val.extend(
                                    values[k_values])
                                col_name = k_values
                                tbl_name = key
                    if tbls_pos[pos] == min(tbls_pos) and tbls_name[pos] == key:
                        tbl_1.extend(values[inter_column[0]])
                pos = 1 if pos == 0 else 0
            if len(tbl_2) > 0 and len(tbl_1) > 0:
                for val_1 in tbl_1:
                    for val_2 in tbl_2:
                        if val_1 == val_2:
                            idx = tbl_2.index(val_2)
                            rep_values.append(idx)
            for idx_val in rep_values:
                if len(new_col_val) > 0:
                    new_col_val[col_name] += [temp_col_val[idx_val]]
                else:
                    new_col_val[col_name] = [temp_col_val[idx_val]]
            default_table[tbl_name][col_name] = new_col_val[col_name]
            for fc_value in from_clause:
                for s_value in select:
                    if s_value['value'][0] == fc_value['name']:
                        col_alias_len = len(fc_value['name']) + 1
                        col_name = s_value['value'][col_alias_len:]
                        col_alias = s_value['name']
                        if 'name' in s_value:
                            default_table[fc_value['value']][col_alias] = default_table[fc_value['value']].pop(
                                col_name)
                            for col in cols_list:
                                if col_name in col:
                                    cols[col_alias] = col[col_name]
                del default_table[fc_value['value']][inter_column[0]]
            final_table = default_table
            tbl_values = list(final_table.values())
            tbl_data_list = []
            temp_val_list = []
            temp_val_dict = {}
            for values in tbl_values:
                for key, vals in values.items():
                    temp_val_list.append(vals)
            for values in temp_val_list:
                for pos, val in enumerate(values):
                    if len(temp_val_dict) > 0 and pos in temp_val_dict.keys():
                        temp_val_dict[pos] += [val]
                    else:
                        temp_val_dict[pos] = [val]
            for val in temp_val_dict.values():
                tbl_data_list.append(val)

        result = self.sql_creating_table_presentation(
            tbl_columns=cols, tbl_data=tbl_data_list)
        print(result)

    def sql_creating_table_presentation(self, tbl_columns: dict, tbl_data: list):
        tbl_head_1, tbl_head_2 = " ", " "
        tbl_body_1, tbl_body_2, tbl_body_full = "", "", ""
        for col in tbl_columns:
            tbl_head_1 += "+" + "-"*(len(col) + 1)
            tbl_head_2 += f"|{col} "
        tbl_head_1 += "+\n"
        tbl_head_2 += "|\n"
        for pos, values in enumerate(tbl_data):
            tbl_body_1 = f"{pos}|"
            for col, value in zip(tbl_columns, values):
                diff_len = (len(col) + 1) - len(value)
                tbl_body_2 += " " * (diff_len) + f"{value}|"
            tbl_body_full += tbl_body_1 + tbl_body_2 + "\n"
            tbl_body_2 = ""
        tbl = tbl_head_1 + tbl_head_2 + tbl_head_1 + tbl_body_full + tbl_head_1
        return tbl

    def sql_file_recovering(self, name: str):
        try:
            if os.path.exists(os.getcwd() + f"/{name}.sql"):

                with open(os.getcwd() + f"/{name}.sql") as f:
                    sql_file_data = f.read()
        except:
            print("Arquivo sql não encontrado.\n")
        else:
            print("Arquivo encontrado e processado.\n")
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

    def _insert_into(self, tbls_names: list, inserts: dict):
        tbl_values = []
        for tbl_name in tbls_names:
            tbl_values = inserts[tbl_name]
            cols = self._columns_data(tbl_name)
            if (tbl_name and tbl_values) is not None:
                Table(name=tbl_name, columns=cols,
                      data=tbl_values).save()

    def _update_table(self, tbls_names: list, updates: dict):
        where_columns = updates['where_column']
        set_columns = updates['set_column']
        set_values = updates['set_value']
        # print(where_columns, set_columns, set_values)
        if len(tbls_names) == len(where_columns) == len(set_columns) == len(set_values):
            for i in range(len(tbls_names)):
                tbl_dict = {}
                where_idx = []
                tbl_values_temp = ()
                tbl_values = []
                cols = self._columns_data(tbls_names[i])
                tbl_data = self._tables_data(tbls_names[i])
                if (cols and tbl_data) is not None:
                    cols_names = list(cols.keys())
                    data_values = [value.split(';')
                                   for value in tbl_data.split()]
                    for pos, name in enumerate(cols_names):
                        for value in data_values:
                            if len(tbl_dict) > 0 and name in tbl_dict.keys():
                                tbl_dict[name] += [value[pos]]
                            else:
                                tbl_dict[name] = [value[pos]]
                    tbl_dict_values = tbl_dict.values()
                    for values in tbl_dict_values:
                        for tbl_pos, tbl_value in enumerate(values):
                            # print(tbl_value, where_columns[i], type(tbl_value))
                            if tbl_value == str(where_columns[i]):
                                where_idx.append(tbl_pos)
                    for idx in where_idx:
                        tbl_dict[set_columns[i]][idx] = set_values[i]
                    tbl_list = list(tbl_dict.values())
                    for idx in range(len(tbl_list[0])):
                        for values in tbl_list:
                            tbl_values_temp += (values[idx],)
                        tbl_values.append(tbl_values_temp)
                        tbl_values_temp = ()
                Table(name=tbls_names[i], columns=cols,
                      data=tbl_values).save()

    def _delete_table(self, tbls_names: list, delete_data: list):
        if (len(tbls_names) == len(delete_data)) and (tbls_names and delete_data) is not None:
            for i in range(len(tbls_names)):
                cols = self._columns_data(tbls_names[i])
                tbl_data = self._tables_data(tbls_names[i])
                data_values = [value.split(';')
                               for value in tbl_data.split()]
                if len(data_values) > 0 and self.commit:
                    for value in data_values:
                        # value[0] -> nm_fruta; value[1] -> tp_fruta
                        if delete_data[i] in value:
                            del data_values[data_values.index(value)]
                    tbl_values = [tuple(value) for value in data_values]
                    Table(name=tbls_names[i], columns=cols,
                          data=tbl_values).save()
        else:
            raise NotImplementedError
