/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;

public class CustomEntityKeyEncoder implements EntityKeyEncoder {

  @Override
  public byte[] encode(NameIdentifier entity) {
    // Simple implementation, just use the entity's identifier as the key
    // We will change this in next PR
    // TODO (yuqi) Information of NameIdentifier may not enough for key encoding, we need to infer
    // object
    //  class from key and then deserialize it when try to get it from kv store.
    return entity.toString().getBytes();
  }

  @Override
  public byte[] encode(Namespace namespace) {
    return namespace.toString().getBytes();
  }
}
