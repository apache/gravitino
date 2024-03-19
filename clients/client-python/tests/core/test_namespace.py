"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import pytest
from gravitino_client.core import Namespace

def test_empty_namespace():
    empty = Namespace()
    assert empty.length() == 0
    assert len(empty.levels()) == 0
    assert empty.is_empty() == True

def test_create_namespace():
    ns = Namespace('catalog', 'schema', 'table')
    assert ns.length() == 3
    assert ns.levels() == ('catalog', 'schema', 'table')
    assert ns.level(0) == 'catalog'
    assert ns.level(1) == 'schema'
    assert ns.level(2) == 'table'
    assert ns.is_empty() == False
    assert ns == Namespace('catalog', 'schema', 'table')
    assert str(ns) == 'catalog.schema.table'

    ns1 = Namespace('a')
    assert ns1.length() == 1
    assert ns1.level(0) == 'a'
    assert ns1.is_empty() == False
    assert ns1 == Namespace('a')
    assert str(ns1) == 'a'


def test_create_illigal_namespace():
    with pytest.raises(AssertionError) as e:
        Namespace('a', None, 'b')

    with pytest.raises(AssertionError) as e:
        Namespace(None)