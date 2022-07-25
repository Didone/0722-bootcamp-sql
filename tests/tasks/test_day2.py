"""Tests for day one tasks"""
import warnings
from csvms.table import Table

def test_concatenacao():
    """https://github.com/Didone/csvms/issues/13"""
    assert [r for r in Table('lista_frutas').Π({'concat':['nm_fruta','tp_fruta']})] == [
        ('banana', 'doce','bananadoce'),
        ('bergamota', 'azedo','bergamotaazedo'),
        ('maçã', 'doce','maçãdoce')
    ]

def test_exponenciacao():
    """https://github.com/Didone/csvms/issues/14"""
    assert [r for r in Table('lista_frutas').Π({'pow':[2,4]})] == [
        ('banana', 'doce',16),
        ('bergamota', 'azedo',16),
        ('maçã', 'doce',16)
    ]

def test_juncao_externa_esquerda():
    """https://github.com/Didone/csvms/issues/28"""
    tbl = Table('lista_frutas').ᗌᐊ(
        Table('tipo_frutas'),
        where={'eq':['lista_frutas.tp_fruta','tipo_frutas.tp_fruta']})
    assert len(tbl)==3
    assert len(tbl.columns)==4
    for row in tbl:
        assert row in [
            ('banana', 'doce', 'doce', 1.5),
            ('maçã', 'doce', 'doce', 1.5),
            ('bergamota', 'azedo', None, None)]

def test_juncao_externa_direita():
    """https://github.com/Didone/csvms/issues/29"""
    tbl = Table('lista_frutas').ᐅᗏ(
        Table('tipo_frutas'),
        where={'eq':['lista_frutas.tp_fruta','tipo_frutas.tp_fruta']})
    assert len(tbl)==3
    assert len(tbl.columns)==4
    for row in tbl:
        assert row in [
            ('banana', 'doce', 'doce', 1.5),
            ('maçã', 'doce', 'doce', 1.5),
            (None, None, 'amargo', 2.0)]

def test_juncao_externa_completa():
    """https://github.com/Didone/csvms/issues/15"""
    tbl = Table('lista_frutas').ᗌᗏ(
        Table('tipo_frutas'),
        where={'eq':['lista_frutas.tp_fruta','tipo_frutas.tp_fruta']})
    assert len(tbl.columns)==4
    assert len(tbl)==4
    for row in tbl:
        assert row in [
            ('banana', 'doce', 'doce', 1.5),
            ('maçã', 'doce', 'doce', 1.5),
            ('bergamota', 'azedo', None, None),
            (None, None, 'amargo', 2.0)]

def test_semi_juncao_esquerda():
    """https://github.com/Didone/csvms/issues/28"""
    try:
        tbl = Table('lista_frutas').ᐅᐸ(
            Table('tipo_frutas'),
            where={'eq':['lista_frutas.tp_fruta','tipo_frutas.tp_fruta']})
        assert len(tbl.columns)==2
        assert len(tbl)==3
        for row in tbl:
            assert row in [
                ('banana', 'doce'),
                ('maçã', 'doce'),
                ('bergamota', 'azedo')]
    except AttributeError:
        warnings.warn(UserWarning('Not implemented'))

def test_semi_juncao_direita():
    """https://github.com/Didone/csvms/issues/28"""
    try:
        tbl = Table('lista_frutas').ᐳᐊ(
            Table('tipo_frutas'),
            where={'eq':['lista_frutas.tp_fruta','tipo_frutas.tp_fruta']})
        assert len(tbl.columns)==2
        assert len(tbl)==3
        for row in tbl:
            assert row in [
                ('doce',1.5),
                ('doce',1.5),
                (None,None)]
    except AttributeError:
        warnings.warn(UserWarning('Not implemented'))
