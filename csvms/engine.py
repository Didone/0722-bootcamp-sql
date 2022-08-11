"""CSVMS SQL Engine Module
See https://github.com/Didone/csvms/discussions/6
"""
from select import select
from mo_sql_parsing import parse
from csvms.table import Table

class Engine():
    """Class used to implement bootcamp tasks"""

    def __init__(self) -> None:
        self.modified_tbls = dict()    

    def execute(self, sql:str, file=False) -> str:
        """Execute SQL statement
        :param sql: String with sql statement"""

        if file:
            f = open(sql, 'r+')
            sql = ''
            for l in f:
                sql += l[:l.find("--")]

        commands = list()

        if ';' in sql:
            if sql[-1] == ';':
                commands = sql.split(';')[:-1]
            else:
                commands = sql.split(';')
        else:
            commands.append(sql)
        
        for command in commands:
            command = command.replace('\n', '')
            if command.replace(' ', '').upper() == 'COMMIT':
                self._save_tables()
            else:
                try:
                    tokens = parse(command)
                except:
                    continue

                if tokens.get('create table') is not None:
                    return self._create_table(
                        tbl_name=tokens['create table']['name'],
                        tbl_columns=tokens['create table']['columns']
                    )
                elif tokens.get('insert') is not None:
                    return self._insert(
                        tbl_name=tokens['insert'], 
                        values=tokens['query']['select']
                    )
                elif tokens.get('update') is not None:
                    return self._update_or_delete(
                        tbl_name=tokens['update'], 
                        set=tokens['set'], 
                        where=tokens['where']
                    )
                elif tokens.get('delete') is not None:
                    return self._update_or_delete(
                        tbl_name=tokens['delete'], 
                        where=tokens['where']
                    )
                elif tokens.get('select') is not None:
                    tbl = self._select(tokens)
                    print(tbl)
                    return tbl.show()
                elif tokens.get('select_distinct') is not None:
                    tbl = self._select(tokens, distinct=True)
                    print(tbl)
                    return tbl.show()
                else:
                    raise NotImplementedError

    def _create_table(self, tbl_name:str, tbl_columns:list) -> str:
        cols = dict()
        for c in tbl_columns:
            cname = c['name']
            ctype = Table.dtypes[list(c['type'].keys())[0]]
            cols[cname]=ctype

        self._save_tables()
        Table(
            name=tbl_name,
            columns=cols
        ).save()
        return f"Table {tbl_name} created"

    def _insert(self, tbl_name:str, values:list) -> str:
        new_row = list()
        tbl = self._load_table(tbl_name)
        for v in values:
            if isinstance(v['value'], dict) :
                new_row.append(v['value']['literal'])
            else:
                new_row.append(v['value'])

        tbl.append(*new_row)
        self.modified_tbls[tbl_name] = tbl
        return f"Inserted into {tbl_name}"

    def _update_or_delete(self, tbl_name:str, set:dict or None = None, where=dict) -> str:
        """Atualiza ou Deleta linhas da tabela baseado em condições passadas como parâmetro\n
           Caso 'set' não receba parâmetros a função agira como DELETE\n
           Caso contrário agira como UPDATE"""
        tbl = self._load_table(tbl_name)

        op_str = list(where.keys())[0]
        op = Table.operations[op_str]

        op_val = None
        op_col = None
        
        if set is not None:
            set_col_val = dict()

        where = where.get(op_str)

        if isinstance(where, list):
            op_col = where[0]
            if isinstance(where[1], dict):
                op_val = where[1].get('literal')
            else:
                op_val = where[1]
        
        if set is not None:
            for k, v in set.items():
                if isinstance(v, dict):
                    v = v['literal']
                set_col_val[k] = v

        for row in range(len(tbl)):
            rlist = tbl[row]
            if op(rlist[op_col], op_val):
                if set is not None:
                    for k, v in set_col_val.items():
                        rlist[k] = v
                    tbl[row] = tuple(rlist.values())
                else:
                    del tbl[row]
        if set is not None:
            return f"Updated {tbl_name}"
        else:
            return f"Deleted from {tbl_name}"
        self.modified_tbls[tbl_name] = tbl

    def _select(self, sql:dict, distinct:bool=False) -> Table:

        def _change_name(table) -> Table:
            if isinstance(table, dict):
                if isinstance(table.get('value'), dict):
                    tbl = self._select(table.get('value'))
                    if table.get('name') is not None:
                        tbl = tbl.ρ(table['name'])
                        return tbl
                    else:
                        return tbl

                if table.get('name') is not None:
                    tbl = Table(table['value'])
                    tbl = tbl.ρ(table['name'])
                    return tbl
            else:
                return Table(table)
        
        f = sql['from']
        result = None

        # FROM E JOIN
        if isinstance(f, list):
            result = _change_name(f[0]) if isinstance(f[0], dict) else Table(f[0])
            for element in f[1:]:
                if isinstance(element, dict):
                    if element.get('value') is not None:
                        result = result * _change_name(element)
                    if element.get('inner join') is not None:
                        result = self._inner_join(result, _change_name(element['inner join']), element['on'])
                    if element.get('right join') is not None or element.get('right outer join') is not None:
                        result = self._right_join(result, _change_name(element[list(element.keys())[0]]), element['on'])
                    if element.get('left join') is not None or element.get('left outer join') is not None:
                        result = self._left_join(result, _change_name(element[list(element.keys())[0]]), element['on'])
                    if element.get('full join') is not None:
                        result = self._full_join(result, _change_name(element['full join']), element['on'])
                    if element.get('select') is not None:
                        result = result * self._select(element, distinct=False)
                    if element.get('select_distinct') is not None:
                        result = result * self._select(element, distinct=True)
                else:
                    result = result * _change_name(element)

                    
        elif isinstance(f, dict):
            result = Table(f['value'])
            result.name = f['name']
        else:
            result = Table(f)

        # WHERE E CONDIÇÕES
        if sql.get('where') is not None:
            result = result.σ(sql['where'])

        # PROJEÇÕES
        if distinct:
            projection = sql['select_distinct']
        else:
            projection = sql['select']

        if isinstance(projection, list):
            for col in projection:
                if isinstance(col.get('value'), dict):
                    result = result.Π(col['value'], col['name'])
            result = result.π(projection)
            if distinct:
                return Table(
                    name=result.name,
                    columns=result.columns,
                    data=list(dict.fromkeys(result._rows))
                    )
            else:
                return result
        elif isinstance(projection, dict):
            if isinstance(projection.get('value'), dict):
                result = result.Π(col['value'], col['name'])
            cols = list()
            cols.append(projection)
            result = result.π(cols)
            if distinct:
                return Table(
                    name=result.name,
                    columns=result.columns,
                    data=list(dict.fromkeys(result._rows))
                    )
            else:
                return result
        else:
            if distinct:
                return Table(
                    name=result.name,
                    columns=result.columns,
                    data=list(dict.fromkeys(result._rows))
                    )
            else:
                return result
    
    def _inner_join(self, table_a:Table, table_b:Table, on:dict) -> Table:
        return table_a.ᐅᐊ(table_b, on)
    
    def _right_join(self, table_a:Table, table_b:Table, on:dict) -> Table:
        return table_a.ᐅᗏ(table_b, on)
    
    def _left_join(self, table_a:Table, table_b:Table, on:dict) -> Table:
        return table_a.ᗌᐊ(table_b, on)
    
    def _full_join(self, table_a:Table, table_b:Table, on:dict) -> Table:
        return table_a.ᗌᗏ(table_b, on)

    def _save_tables(self):
        """Salva as mudanças feitas nas tabelas
           que estão armazenadas na variável modified_tbls"""
        for tbl in self.modified_tbls.values():
            tbl.save()
            print('Tabela', tbl.name, 'foi salva no SCHEMA', tbl.database.name)
        self.modified_tbls.clear()

    def _load_table(self, tbl_name:str) -> Table:
        if self.modified_tbls.get(tbl_name) is not None:
            return self.modified_tbls[tbl_name]
        else:
            return Table(tbl_name)

