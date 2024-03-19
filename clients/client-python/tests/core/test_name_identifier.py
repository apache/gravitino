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