"""Tests for day one tasks"""
from os.path import exists
from csvms.engine import Engine
from csvms.catalog import Catalog
from csvms.schema import Database
from csvms.table import Table

eng = Engine()

def test_criacao():
    """https://github.com/Didone/csvms/issues/7"""
    eng.execute("""
    CREATE TABLE lista_frutas (
        nm_fruta TEXT ,
        tp_fruta TEXT 
    )""")
    assert not exists(f"{Database.FILE_DIR}/{Database.DEFAULT_DB}/lista_frutas")
    assert Catalog(Database.FILE_DIR).objects[f'{Database.DEFAULT_DB}.lista_frutas'] == {
        'name': f'{Database.DEFAULT_DB}.lista_frutas',
        'columns': {
            'nm_fruta': 'text',
            'tp_fruta': 'text'},
        'indexes': {}}
    eng.execute("""
    CREATE TABLE tipo_frutas (
        tp_fruta TEXT,
        vl_fruta FLOAT
    )""")
    assert not exists(f"{Database.FILE_DIR}/{Database.DEFAULT_DB}/tipo_frutas")
    assert Catalog(Database.FILE_DIR).objects[f'{Database.DEFAULT_DB}.tipo_frutas'] == {
        'name': f'{Database.DEFAULT_DB}.tipo_frutas',
        'columns': {
            'tp_fruta': 'text',
            'vl_fruta': 'float'},
        'indexes': {}}

def test_inclusao():
    """https://github.com/Didone/csvms/issues/8"""
    eng.execute("""
    INSERT INTO lista_frutas VALUES ('banana','doce');
    INSERT INTO lista_frutas VALUES ('limão','amargo');
    INSERT INTO lista_frutas VALUES ('bergamota','azedo');
    INSERT INTO lista_frutas VALUES ('maçã','doce');
    COMMIT;""")
    assert [r for r in Table(f'{Database.DEFAULT_DB}.lista_frutas')] == [
        ('banana', 'doce'),
        ('limão', 'amargo'),
        ('bergamota', 'azedo'),
        ('maçã', 'doce')]

def test_atualizacao():
    """https://github.com/Didone/csvms/issues/9"""
    eng.execute("""
    UPDATE lista_frutas
    SET tp_fruta = 'azedo'
    WHERE nm_fruta = 'limão';
    COMMIT;""")
    assert [r for r in Table(f'{Database.DEFAULT_DB}.lista_frutas')] == [
        ('banana', 'doce'),
        ('limão', 'azedo'),
        ('bergamota', 'azedo'),
        ('maçã', 'doce')]

def test_insercao():
    """https://github.com/Didone/csvms/issues/10"""
    eng.execute("""
    INSERT INTO tipo_frutas VALUES ('doce',1.5);
    INSERT INTO tipo_frutas VALUES ('amargo',2.0);
    COMMIT;""")
    assert [r for r in Table(f'{Database.DEFAULT_DB}.tipo_frutas')] == [
        ('doce',1.5),
        ('amargo',2.0)]

def test_delecao():
    """https://github.com/Didone/csvms/issues/11"""
    eng.execute("""
    DELETE FROM lista_frutas 
    WHERE nm_fruta = 'limão';
    COMMIT;
    """)
    assert [r for r in Table(f'{Database.DEFAULT_DB}.lista_frutas')] == [
        ('banana', 'doce'),
        ('bergamota', 'azedo'),
        ('maçã', 'doce')]
