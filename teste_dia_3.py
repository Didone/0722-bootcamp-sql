from csvms.engine import Engine
from mo_sql_parsing import parse

eng = Engine()
# sql = """SELECT tp_fruta "tipo de fruta" FROM lista_frutas frutas"""
# sql = """SELECT l.nm_fruta AS fruta
#      , t.vl_fruta AS valor
#   FROM lista_frutas l
#      , tipo_frutas t
#  WHERE l.tp_fruta = t.tp_fruta"""
# sql = """SELECT v.cod_venda
#      , l.tp_fruta
#   FROM venda_frutas v
#   INNER JOIN lista_frutas l ON v.nm_fruta = l.nm_fruta"""
# sql = """SELECT v.cod_venda
#      , l.nm_fruta
#      , v.qtd_venda
#      , t.vl_fruta
#      , t.vl_fruta * v.qtd_venda as total
#   FROM venda_frutas v
#   INNER JOIN lista_frutas l ON v.nm_fruta = l.nm_fruta
#   INNER JOIN tipo_frutas t ON l.tp_fruta = t.tp_fruta"""
# sql = """SELECT t.tp_fruta `tipos sem frutas`
#   FROM lista_frutas l
#  RIGHT JOIN tipo_frutas t ON t.tp_fruta = l.tp_fruta
#  WHERE l.tp_fruta is null"""
sql = """SELECT DISTINCT f.nm_fruta AS "Frutas não vendidas"
  FROM lista_frutas f
  LEFT JOIN (
SELECT v.nm_fruta
     , p.vl_fruta * v.qtd_venda as total
  FROM venda_frutas v
  INNER JOIN lista_frutas l ON v.nm_fruta = l.nm_fruta
  INNER JOIN tipo_frutas p ON l.tp_fruta = p.tp_fruta) c ON f.nm_fruta = c.nm_fruta
  WHERE c.total < 20 
     OR c.total IS NULL"""
print("\n\n",parse(sql),"\n\n")
eng.execute(sql)