"""Catalog test cases"""
from os.path import exists
from os import environ
import pytest
from csvms.table import Table

@pytest.fixture()
def tbl():
    """Table tbl definition"""
    table = Table(
    name="pytest.test",
    columns={
        "chave":int,
        "desc":str,
        "valor":float},
    data=[
        (1,"a",0.55),
        (2,"b",1.05),
        (3,"c",9.99)])
    yield table
    if table.save():
        table.drop()

def test_database(tbl):
    """Test database properties"""
    assert str(tbl.database.location).startswith(environ['CSVMS_FILE_DIR'])
    assert tbl.database.name == "pytest"

def test_table_data(tbl):
    """Test all magic methods"""
    assert len(tbl) == 3
    assert [row for row in tbl] == [(1,"a",0.55),(2,"b",1.05),(3,"c",9.99)]
    assert tbl[0] == {"chave":int(1), "desc":str("a"), "valor": float(0.55)}
    assert tbl[1] == {"chave":int(2), "desc":str("b"), "valor": float(1.05)}
    assert tbl[2] == {"chave":int(3), "desc":str("c"), "valor": float(9.99)}
    assert tbl[3] == {'chave': None, 'desc': None, "valor": None} # Out of index
    return True

def test_input_output(tbl):
    """ Teste table Imput and Outup """
    assert tbl.location == f"{tbl.database.location}/{tbl.name}.{Table._FORMAT_}"
    assert tbl.save()
    assert exists(tbl.database.catalog.location)
    assert tbl.database.catalog[tbl.full_name] == {
        'name': 'pytest.test',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float'},
        'indexes': {}}
    assert test_table_data(Table(f"{tbl.full_name}"))
    assert tbl.drop()
    assert not exists(tbl.location)


def test_alter_table(tbl):
    """ Test table definition alterations """
    assert tbl.alter("add", {"nova":int})
    assert [row for row in tbl] == [(1,"a",0.55,0),(2,"b",1.05,0),(3,"c",9.99,0)]
    assert tbl.alter("modify", {"nova":int}, {"coluna":str})
    assert [row for row in tbl] == [(1,"a",0.55,"0"),(2,"b",1.05,"0"),(3,"c",9.99,"0")]
    assert tbl.alter("drop", {"coluna":str})
    assert [row for row in tbl] == [(1,"a",0.55),(2,"b",1.05),(3,"c",9.99)]

def test_data_manipulation(tbl):
    """ Tert table DMLs """
    del tbl[0]
    assert len(tbl) == 2
    assert tbl.append(4,"d",0)
    assert len(tbl) == 3
    tbl[0]=(5,"e",2.2)
    assert tbl[0] == {"chave":int(5), "desc":str("e"), "valor": float(2.2)}
