from csvms.engine import Engine

eng = Engine()
sql = """"""

while(True):
    sql = input(">")
    if sql == 'exit':
        break
    try:
        eng.execute(sql)
    except Exception as e:
        print(e)
        continue
