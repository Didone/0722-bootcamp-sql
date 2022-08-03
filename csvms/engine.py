"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""

from typing import List
from mo_sql_parsing import parse
from csvms.table import Table

class Engine():

    """Class used to implement bootcamp tasks"""

    commitStatus = False

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
        elif sql.lower().find('update') >= 0:
            queryToParse = ''
            sql = sql.split("\n")
            del sql[0]
            for i in range(len(sql)):
                if sql[i].find('COMMIT')<0:
                    sql[i] = sql[i].replace(";","")
                    sql[i] = sql[i].replace("\n","")
                    sql[i] = sql[i].strip()
                    print(sql[i])
                    queryToParse = queryToParse + " " + sql[i]
            queryToParse = queryToParse.strip()
            return self._update_table(queryToParse)
        elif sql.lower().find('delete from') >= 0:
            queryToParse = ''
            sql = sql.split("\n")
            del sql[0]
            for i in range(len(sql)):
                if sql[i].find('COMMIT')<0:
                    sql[i] = sql[i].replace(";","")
                    sql[i] = sql[i].replace("\n","")
                    sql[i] = sql[i].strip()
                    print(sql[i])
                    queryToParse = queryToParse + " " + sql[i]
            queryToParse = queryToParse.strip()
            return self._delete_table(queryToParse)
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
    
    def _update_table(self, queryToParse:str):
        ast = parse(queryToParse)
        tblName = ast['update']
        oldValue = ast['where']['eq'][1]['literal']
        newValueCol = ast['set']
        newValueCol = list(newValueCol.items())[0][0]
        newValue = ast['set'][newValueCol]
        newValue = list(newValue.items())[0][1]
        oldValueCol = ast['where']['eq'][0]
        tbl = Table(tblName)
        for idx in range(len(tbl)):
            if tbl[idx][oldValueCol]==oldValue:
                row = tbl[idx]
                row[newValueCol]=newValue
                tbl[idx] = tuple(row.values())
        tbl.save()
        return f"Os valores na tabela {tblName} foram atualizados com sucesso"

    def _delete_table(self, queryToParse:str):
        ast = parse(queryToParse)
        tbl_name = ast['delete']
        deletionCol = ast['where']['eq'][0]
        deletionRow = ast['where']['eq'][1]['literal']
        tbl = Table(tbl_name)
        print(tbl)
        for idx in reversed(range(len(tbl))):
            if tbl[idx][deletionCol] == deletionRow:
                del tbl[idx]
        tbl.save()
