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

def test_create_namespace():
    ns = Namespace('catalog', 'schema', 'table')
    assert ns.length() == 3
    assert ns.levels() == ('catalog', 'schema', 'table')
    assert ns.level(0) == 'catalog'
    assert ns.level(1) == 'schema'
    assert ns.level(2) == 'table'

    with pytest.raises(AssertionError) as e:
        Namespace('a', None, 'b')
