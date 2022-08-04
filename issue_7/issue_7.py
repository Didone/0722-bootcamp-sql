from mo_sql_parsing import parse
from csvms.table import Table
from csvms.engine import Engine

eng = Engine()

eng.execute('issue_7/frutas.sql.txt', file=True)
