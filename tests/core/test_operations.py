"""Test Operators"""
import warnings
from csvms.table import Table

A = Table(
    name="pytest.A",
    columns={
        "chave":int,
        "desc":str,
        "valor":float},
    data=[
        (1,"a",0.55),
        (3,"c",None)])

B = Table(
    name="pytest.B",
    columns={
        "chave":int,
        "desc":str,
        "valor":float},
    data=[
        (1,"a",0.55),
        (2,"b",1.05)])

def test_union():
    """Test union operator"""
    assert (A + B).definition == {
        "name": "mock.(A∪B)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"},
        'indexes': {}}
    assert [r for r in (A + B)] == [
        (1,"a",0.55),
        (3,"c",None),
        (2,"b",1.05)
    ]

def test_inserct():
    """Test intersection operator"""
    assert (A % B).definition == {
        "name": "mock.(A∩B)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"},
        'indexes': {}}
    assert [r for r in (A % B)] == [(1,"a",0.55)]

def test_diff():
    """Test diff operator"""
    assert (A - B).definition == {
        "name": "mock.(A−B)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"},
        'indexes': {}}
    assert [r for r in (A - B)] == [(3,"c",None)]

    assert (B - A).definition == {
        "name": "mock.(B−A)",
        "columns": {
            "chave": "integer",
            "desc": "text",
            "valor": "float"},
        'indexes': {}}
    assert [r for r in (B - A)] == [(2,"b",1.05)]

def test_product():
    """Test product operator"""
    assert (A * B).definition == {
        "name": "mock.(A×B)",
        "columns": {
            "A.chave": "integer","A.desc": "text","A.valor": "float",
            "B.chave": "integer","B.desc": "text","B.valor": "float"},
        'indexes': {}}
    assert [r for r in (A * B)] == [
        (1, 'a', 0.55, 1, 'a', 0.55),
        (1, 'a', 0.55, 2, 'b', 1.05),
        (3, 'c', None, 1, 'a', 0.55),
        (3, 'c', None, 2, 'b', 1.05)
    ]

def test_selection():
    """Test selection operator"""
    assert A.σ({"eq":['chave',1]}).definition == {
        'name': 'mock.(Aσ)',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float'},
        'indexes': {}}
    assert [r for r in A.σ({'and':[
            {"eq":['chave',1]},
            {"exists":'valor'}]})] == [(1,"a",0.55)]
    assert [r for r in A.σ({'or':[
            {"eq":['chave',1]},
            {"missing":'valor'}]})] == [(1,"a",0.55),(3,"c",None)]
    assert [r for r in A.σ({"lt":['chave',3]})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"gt":['chave',1]})] == [(3,"c",None)]
    assert [r for r in A.σ({"eq":['chave',1]})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"lte":['chave',3]})] == [(1,"a",0.55),(3,"c",None)]
    assert [r for r in A.σ({"gte":['chave',1]})] == [(1,"a",0.55),(3,"c",None)]
    assert [r for r in A.σ({"neq":['chave',1]})] == [(3,"c",None)]
    assert [r for r in A.σ({"is":['valor',None]})] == [(3,"c",None)]
    assert [r for r in A.σ({"missing":'valor'})] == [(3,"c",None)]
    assert [r for r in A.σ({"exists":'valor'})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"in":['desc','ab']})] == [(1,"a",0.55)]
    assert [r for r in A.σ({"nin":['desc','ab']})] == [(3,"c",None)]

def test_projection():
    """Test projection operator"""
    assert A.π([{'value':'chave'}]).definition == {
        'name': 'mock.(Aπ)',
        'columns': {'chave': 'integer'},
        'indexes': {}}
    assert [r for r in A.π([{'value':'chave'}])] == [(1,), (3,)]
    assert A.π([{'value':'chave','name':'key'}]).definition == {
        'name': 'mock.(Aπ)',
        'columns': {'key': 'integer'},
        'indexes': {}}
    assert [r for r in A.π([{'value':'chave','name':'key'}])] == [(1,), (3,)]

def test_extended_projection():
    """Test extended projection operator"""
    assert A.Π(100,'%').definition == {
        'name': 'mock.(AΠ)',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float',
            '%': 'integer'},
        'indexes': {}}
    assert [r for r in A.Π(100,'%')] == [(1,"a",0.55,100),(3,"c",None,100)]
    assert [r for r in A.Π({'add':['valor',2]})] == [(1,'a',0.55,2.55),(3,'c',None,None)]
    assert [r for r in A.Π({'sub':['valor',2]})] == [(1,'a',0.55,-1.45),(3,'c',None,None)]
    assert [r for r in A.Π({'div':['valor',2]})] == [(1, 'a', 0.55, 0.275),(3,'c',None,None)]
    assert [r for r in A.Π({'mul':['valor',2]})] == [(1,'a',0.55,1.1),(3,'c',None,None)]

def test_extended_projection_concat():
    """Test function on extended projection"""
    if Table.functions.get('concat') is not None:
        assert [r for r in A.Π({'concat':['desc','valor']})] == [
            (1,'a',0.55,'a0.55'),
            (3,'c',None,None)]
    else:
        warnings.warn(UserWarning('Not implemented'))

def test_extended_projection_pow():
    """Test function on extended projection"""
    if Table.functions.get('pow') is not None:
        assert [r for r in A.Π({'pow':['chave',8]})] == [
            (1,'a',0.55,1),
            (3,'c',None,6561)]
    else:
        warnings.warn(UserWarning('Not implemented'))

def test_rename_projection():
    """Test rename projection operator"""
    assert A.ρ("C").definition == {
        'name': 'mock.C',
        'columns': {
            'chave': 'integer',
            'desc': 'text',
            'valor': 'float'},
        'indexes': {}
    }
