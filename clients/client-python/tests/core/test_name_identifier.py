"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import pytest
from gravitino_client.core import NameIdentifier,Namespace

def test_create_name_identifier():
    id1 = NameIdentifier.of('a', 'b', 'c')
    assert id1.hasNamespace()
    assert id1.namespace() == Namespace('a', 'b')
    assert id1.name() == 'c'
    
    id2 = NameIdentifier(Namespace('a', 'b'), 'c')
    assert id2.hasNamespace()
    assert id2.namespace() == Namespace('a', 'b')
    assert id2.name() == 'c'
    assert str(id2) == 'a.b.c'

    id3 = NameIdentifier.of('a', 'b')
    assert id3.hasNamespace()
    assert id3.namespace() == Namespace('a')
    assert id3.name() == 'b'
    assert str(id3) == 'a.b'

    id4 = NameIdentifier.of('a')
    assert not id4.hasNamespace()
    assert id4.namespace() == Namespace()
    assert id4.name() == 'a'
    assert str(id4) == 'a'

def test_parse_name_identifier():
    id1 = NameIdentifier.parse('a.b.c')
    assert id1.hasNamespace()
    assert id1.namespace() == Namespace('a', 'b')
    assert id1.name() == 'c'
    assert id1 == NameIdentifier.of('a', 'b', 'c')

    id2 = NameIdentifier.parse('a')
    assert not id2.hasNamespace()
    assert id2.namespace() == Namespace()
    assert id2.name() == 'a'
    assert id2 == NameIdentifier.of('a')
    assert str(id2) == 'a'

def test_illegal_name_identifier():
    with pytest.raises(AssertionError) as e:
        NameIdentifier.of(None, 'b', 'c')
    
    with pytest.raises(AssertionError) as e:
        NameIdentifier.of('a', 'b', None)

    with pytest.raises(AssertionError) as e:
        NameIdentifier.of(None, 'a')
    
    with pytest.raises(AssertionError) as e:
        NameIdentifier.of('a', None)
    
    with pytest.raises(AssertionError) as e:
        NameIdentifier.of(None)

    with pytest.raises(AssertionError) as e:
        NameIdentifier.of()