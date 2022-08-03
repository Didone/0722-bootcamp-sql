"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""

from typing import List
from mo_sql_parsing import parse
from csvms.table import Table

class Engine():

    """Class used to implement bootcamp tasks"""

    def execute(self, sql:str):
        if sql.lower().find('create table') >= 0:
            ast = parse(sql)
            return self._create_table(
                tbl_name=ast['create table']['name'],
                tbl_columns=ast['create table']['columns']
            )
        elif sql.lower().find('insert into') >= 0:
            if sql.lower().find('commit') >= 0:
                commitStatus = True
            sql = sql.replace("\n","")
            querySplit = sql.split(";")
            querySplit.pop()
            return self._insert_table(querySplit,commitStatus)
        else:  
            return None

    def _create_table(self, tbl_name:str, tbl_columns:list):
        cols = dict()
        for _c_ in tbl_columns:
            cname = _c_['name']
            ctype = Table.dtypes[list(_c_['type'].keys())[0]]
            cols[cname]=ctype
        Table(name=tbl_name, columns=cols).save()
        return f"A Tabela {tbl_name} foi criada com sucesso!"

    def _insert_table(self, querySplit:list, commitStatus:bool):
        newValueList = list()
        for i in range(len(querySplit)):
            querySplit[i] = querySplit[i].strip()
            if querySplit[i] != 'COMMIT':    
                ast = parse(querySplit[i])
                tbl_name = ast['insert']
                tbl = Table(tbl_name)
                newValue = ast['query']['select']
                for j in range(len(newValue)):
                    if isinstance(newValue[j]['value'],dict):
                        newValueList.append(newValue[j]['value']['literal'])
                    if isinstance(newValue[j]['value'],float):
                        newValueList.append(newValue[j]['value'])
        for k in range(0, len(newValueList), 2):
            tbl.append(newValueList[k],newValueList[k+1])
        if commitStatus == True:
            tbl.save()
        return f"Os valores na tabela {tbl_name} foram inseridos com sucesso"
    
