/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.storage.EntityKeyEncoder;
import java.io.IOException;
import java.util.List;

/** Kv entity key encoder that encode entities to storage in key-value */
public interface KvEntityKeyEncoder extends EntityKeyEncoder<byte[]> {

  /**
   * Get the byte array prefix of all parents of the given entity that store in key-value store. For
   * example, if the entity is 'metalake1'.'catalog2','schema3', then parents are 'metalake1' and
   * 'metalake1'.'catalog2'. For more please refer to {@link EntityKeyEncoder#encode(NameIdentifier,
   * EntityType, boolean)}
   *
   * @param ident entity identifier of the entity
   * @param entityType the type of the entity
   * @return the byte array prefix of all parents of the given entity
   * @throws IOException
   */
  List<byte[]> getParentsPrefix(NameIdentifier ident, EntityType entityType) throws IOException;

  /**
   * Get the byte array prefix of all children of the given entity that store in key-value store.
   * For example, if the entity is 'metalake1'.'catalog2','schema3', then children are
   * 'metalake1'.'catalog2'.'schema3'.'table4' and 'metalake1'.'catalog2'.'schema3'.'table5'. For
   * more, please refer to {@link EntityKeyEncoder#encode(NameIdentifier, EntityType, boolean)}
   *
   * @param ident entity identifier of the entity
   * @param entityType the type of the entity
   * @return the byte array prefix of all children of the given entity
   * @throws IOException
   */
  List<byte[]> getChildrenPrefix(NameIdentifier ident, EntityType entityType) throws IOException;
}
