from csvms.engine import Engine

eng = Engine()

eng.execute('issue_7/frutas.sql.txt', file=True)
