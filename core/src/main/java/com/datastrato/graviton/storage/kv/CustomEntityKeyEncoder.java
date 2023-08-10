/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;

/**
 * CustomEntityKeyEncoder is an implementation of the EntityKeyEncoder interface that provides
 * methods to encode NameIdentifier and Namespace objects into byte arrays for use as keys in a
 * key-value store.
 */
public class CustomEntityKeyEncoder implements EntityKeyEncoder {

  /**
   * Encodes a NameIdentifier object into a byte array for use as a key in a key-value store.
   *
   * @param entity The NameIdentifier object to be encoded.
   * @return The byte array representing the encoded key.
   */
  @Override
  public byte[] encode(NameIdentifier entity) {
    // A simple approach is taken, utilizing the identifier
    // of the entity as the key. However. This will be improved in future PRs.
    // TODO (yuqi) While the NameIdentifier provides key information, it may
    // be insufficient for key encoding. An improvement
    // involves inferring the object's class from the key, which would help
    //  deserialization when retrieving it from the key-value store.
    return entity.toString().getBytes();
  }

  /**
   * Encodes a Namespace object into a byte array for use as a key in a key-value store.
   *
   * @param namespace The Namespace object to be encoded.
   * @return The byte array representing the encoded key.
   */
  @Override
  public byte[] encode(Namespace namespace) {
    return namespace.toString().getBytes();
  }
}
