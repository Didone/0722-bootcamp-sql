from mo_sql_parsing import parse
from csvms.table import Table
import csv

class Engine():
    """Class used to implement bootcamp tasks"""

    def execute(self, sql:str):
        
        lista_sql = []
        for statement in sql.split(';'):
            if statement == '':
                pass
            elif statement == 'COMMIT':
                pass
            else:
               lista_sql.append(parse(statement))
        
        for query in range(0,len(lista_sql)):
            ast = lista_sql[query]
            if ast.get('create table') is not None:
                return self._create_table(
                    tbl_name=ast['create table']['name'],
                    tbl_columns=ast['create table']['columns']
                )
            elif ast.get('drop') is not None:
                self._drop_table(ast['drop']['table'])
            elif ast.get('query') is not None:
                self.insert(
                    tbl_name=ast['insert'],
                    values=ast['query']['select']
                    )
            elif ast.get('set') is not None:
                self.update(
                    tbl_name=ast['update'],
                    set=ast['set'],
                    where=ast['where']['eq'][1]
                )
            elif ast.get('delete') is not None:
                self.delete(
                    tbl_name=ast['delete'],
                    where=ast['where']['eq'][1]['literal']
                )
            elif ast.get('select') is not None or ast.get('select_distinct') is not None:
                self.select(
                    sql=ast
                    )
            else:
                raise NotImplementedError

    def _create_table(self, tbl_name:str, tbl_columns:list):
        cols = dict()
        for _c_ in tbl_columns:
            cname = _c_['name']
            ctype = Table.dtypes[list(_c_['type'].keys())[0]]
            cols[cname]=ctype
        Table(name=tbl_name, columns=cols).save()
        print( f" Table {tbl_name} was created")


    def insert(self,tbl_name,values):
        #Terminar de implementar para n valores
        try:
            if len(values)> 2: 
                val_int = list()
                for i in values:
                    if type(i['value']) == int :
                        val_int.append(i['value'])
                    elif type(i['value']) == float:
                        pass
                    else:
                        val_str = i['value']['literal'] 

                with open(f"data/default/{tbl_name}.csv",'a+',encoding='utf8',newline='') as lis:
                    writer = csv.writer(lis,delimiter=';')
                    writer.writerow([val_int[0],val_str,val_int[1]])
                print(f'Row  ({val_int[0]},{val_str},{val_int[1]}) inserted in {tbl_name}')

            else: 
                val1=values[0]['value']['literal']
                val2=values[1]['value']

                if type(val2) == float:
                    pass
                else:
                    val2 = val2['literal']

                with open(f"data/default/{tbl_name}.csv",'a+',encoding='utf8',newline='') as lis:
                    writer = csv.writer(lis,delimiter=';')
                    writer.writerow([val1,val2])

                print(f'Row  ({val1},{val2}) inserted in {tbl_name}')
        except:
            raise NotImplementedError

    def update(self,tbl_name,set,where):
        try:
            set = set[list(set.keys())[0]]['literal']
            where = where['literal']
           
            text = open(f"data/default/{tbl_name}.csv", "r+",encoding='utf8')
            text = ''.join([i for i in text])
            dic = dict()
                
            for item in text.split():
                values = item.split(';')
                dic[values[0]] = values[1]
                if where in values:
                    old_val = values[1]
                    x = open(f"data/default/{tbl_name}.csv","w+",encoding='utf8') 
                    text = text.replace(old_val, set,1)
                    x.writelines(text) 
                    x.close() 
                else:
                    pass
            print(f"Row {list(dic.values()).index(old_val) + 1} updated with values ({where},{old_val})")
        except:
            raise NotImplementedError
            
    def delete(self,tbl_name,where):
        try:
            text = open(f"data/default/{tbl_name}.csv", "r+",encoding='utf8')
            text = ''.join([i for i in text])

            values = list()
            removed_value = list()
            for item in text.split():
                if where in item:
                    
                    removed_value.append(item)
                else:
                    values.append(item)

            x = open(f"data/default/{tbl_name}.csv","w+",encoding='utf8') 
            x.writelines('') 
            x.close()

            old_value = removed_value[0].split(';')
                
            for j in values:
                with open(f"data/default/{tbl_name}.csv",'a+',encoding='utf8',newline='') as lis:
                    writer = csv.writer(lis)
                    writer.writerow([j])

            print(f'Row ({old_value[0]},{old_value[1]}) deleted')
        except:
            raise NotImplementedError
                   

    def select(self,sql):
        try:
            if list(sql.keys())[0] == 'select_distinct':

                lf = sql['from'][0]['value']
                vf = sql['from'][1]['left join']['value']['from'][0]['value']
                tf = sql['from'][1]['left join']['value']['from'][2]['inner join']['value']

            
                l = Table(name=f'default.{lf}')
                v = Table(name=f'default.{vf}')
                t = Table(name=f'default.{tf}')

                colum1 = sql['select_distinct']['value'].split('.')[1]
                colum2= sql['from'][1]['left join']['value']['select'][1]['value']['mul'][0].split('.')[1]
                colum3 = sql['from'][1]['left join']['value']['select'][1]['value']['mul'][1].split('.')[1]
                colum4 = sql['from'][1]['left join']['value']['from'][2]['on']['eq'][0].split('.')[1]
                alias1 = sql['select_distinct']['name']
                alias2 = sql['from'][1]['left join']['value']['select'][1]['name']
                alias3 = sql['from'][1]['left join']['name']

                operador =list(sql['from'][1]['left join']['value']['select'][1]['value'].keys())[0]

                c = (v.ᐅᐊ(l,where={'eq':[f'{vf}.{colum1}',f'{lf}.{colum1}']})).ᐅᐊ(
                    t,where={'eq':[f'{lf}.{colum4}',f'{tf}.{colum4}']}).Π(
                    {f'{operador}':[f'{tf}.{colum2}',f'{vf}.{colum3}']},f'{alias2}').π(
                    [{'value':f'{vf}.{colum1}'},{'value':f'{alias2}'}]
                    ).ρ(f'{alias3}').ᐅᗏ(
                                l,where={'eq':[f'{alias3}.{colum1}',f'{lf}.{colum1}']}
                                ).σ(
                                    {'lt':[f'{alias3}.{alias2}',20],'or':{'missing':f'{alias3}.{alias2}'}}
                                ).π(
                                    [{'value':f'{lf}.{colum1}','name':f'{alias1}'}]).distinct(f'{lf}.{colum1}')
                                    
                            
                    
                return print(c)

            else:
                if sql['select'] == "*":
                    tbl = Table(
                        name=f"default.{sql['from']}",
                    )
                    return print(tbl)
                elif len(sql['select']) > 2:
                    
                    lf = sql['from'][1]['inner join']['value']
                    tf = sql['from'][2]['inner join']['value']
                    vf = sql["from"][0]["value"]

                    colum_vf1=sql['select'][0]['value'].split('.')[1]
                    colum_igual=sql['select'][1]['value'].split('.')[1]
                    colum_vf2=sql['select'][2]['value'].split('.')[1]
                    colum_tf1=sql['select'][3]['value'].split('.')[1]
                    alias =sql['select'][4]['name']
                    cond = list(sql['select'][4]['value'].keys())[0]

                    l =  Table(name=f'default.{lf}')
                    t =  Table(name=f'default.{tf}')
                    v =  Table(name=f'default.{vf}')

                    mult_join = (v.ᐅᐊ(l,where={'eq':[f'{vf}.{colum_igual}',f'{lf}.{colum_igual}']})).ᐅᐊ(
                        t,where={'eq':[f'{lf}.tp_fruta',f'{tf}.tp_fruta']}
                        ).π(
                            [{'value':f'{vf}.{colum_vf1}'},{'value':f'{lf}.{colum_igual}'},{'value':f'{vf}.{colum_vf2}'},{f'value':f'{tf}.{colum_tf1}'}]).Π(
                            {f'{cond}':[f'{tf}.{colum_tf1}',f'{vf}.{colum_vf2}']},f'{alias}')

                    return print(mult_join)

                elif list(sql['from'][1].keys())[0] == 'inner join':

                    lf = sql['from'][1]['inner join']['value']
                    vf = sql['from'][0]['value']

                    l = Table(name=f'default.{lf}')
                    v = Table(name=f'default.{vf}')

                    colum_vf = sql['select'][0]['value'].split('.')[1]
                    colum_lf = sql['select'][1]['value'].split('.')[1]
                    colum_igual = sql['from'][1]['on']['eq'][0].split('.')[1]

                    inner_join = (v.ᐅᐊ(l,where={'eq':[f'{vf}.{colum_igual}',f'{lf}.{colum_igual}']})).π([{'value':f'{vf}.{colum_vf}'},{'value':f'{lf}.{colum_lf}'}])

                    return print(inner_join)

                elif list(sql['from'][1].keys())[0] == 'right join':

                    lf = sql['from'][0]['value']
                    tf = sql['from'][1]['right join']['value']

                    l = Table(name=f'default.{lf}')
                    t = Table(name=f'default.{tf}')

                    colum = sql['select']['value'].split('.')[1]
                    alias = sql['select']['name']
                    cond = list(sql['where'].keys())[0]

                    right_join = (l.ᐅᗏ(t,where={'eq':[f'{lf}.{colum}',f'{tf}.{colum}']})).σ({f'{cond}':f'{lf}.{colum}'}).π([{'value':f'{tf}.{colum}','name':f'{alias}'}])

                    return print(right_join)
                    
                elif sql['where']['eq'][0].split('.')[1] == sql['where']['eq'][1].split('.')[1]:
                    
                    lf =sql['from'][0]['value']
                    tf =sql['from'][1]['value']

                    alias_tf = sql['select'][1]['name']
                    alias_lf = sql['select'][0]['name']

                    colum_tf = sql['select'][1]['value'].split('.')[1]
                    colum_lf = sql['select'][0]['value'].split('.')[1]

                    colum_igual = sql['where']['eq'][0].split('.')[1]
                    tbl1 = Table(name=f"default.{str(lf)}",)
                    tbl2 = Table(name=f"default.{str(tf)}",)

                    cross = (tbl2*tbl1).σ({'eq':[f'{tf}.{colum_igual}',f'{lf}.{colum_igual}']}).π([{'value':f'{lf}.{colum_lf}','name':f'{alias_lf}'},
                            {'value':f'{tf}.{colum_tf}','name':f'{alias_tf}'}])

                    return print(cross)
                
                else:
                    pass
        except:
            raise NotImplementedError

