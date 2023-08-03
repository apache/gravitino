/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Entity.EntityType.CATALOG;
import static com.datastrato.graviton.Entity.EntityType.METALAKE;
import static com.datastrato.graviton.Entity.EntityType.SCHEMA;
import static com.datastrato.graviton.Entity.EntityType.TABLE;

import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.util.ByteUtils;
import com.datastrato.graviton.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;

/**
 * Encode entity key for KV backend, e.g., RocksDB. The key is used to store the entity in the
 * backend. The final key will be:
 *
 * <pre>
 *     Key                                Value
 * ml_{ml_id}               ----->    matalake info
 * ml_{ml_id}               ----->    matalake info
 * ca_{ml_id}_{ca_id}       ----->    catalog_info
 * ca_{ml_id}_{ca_id}       ----->    catalog_info
 * sc_{ml_id}_{ca_id}_{sc_id} --->    schema_info
 * sc_{ml_id}_{ca_id}_{sc_id} --->    schema_info
 * br_{ml_id}_{ca_id}_{br_id} --->    broker_info
 * br_{ml_id}_{ca_id}_{br_id} --->    broker_info
 *
 * ta_{sc_id}_{table_id}    ----->    table_info
 * ta_{sc_id}_{table_id}    ----->    table_info
 * to_{br_id}_{to_id}       ----->    topic_info
 * to_{br_id}_{to_id}       ----->    topic_info
 * </pre>
 */
public class CustomEntityKeyEncoder implements EntityKeyEncoder {

  // name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX = "name_".getBytes();

  // id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX = "id_".getBytes();

  // Store next usable id, 0 if not exists, e.g., next_usable_id -> 1
  @VisibleForTesting public static final byte[] NEXT_USABLE_ID = "next_usable_id".getBytes();

  @VisibleForTesting public static final byte[] NAMESPACE_SEPARATOR = "_".getBytes();

  private final KvBackend backend;

  public CustomEntityKeyEncoder(KvBackend backend) {
    this.backend = backend;
  }

  /**
   * Get or greate id for name. If the name is not in the storage, create a new id for it. The id is
   * the current max id + 1.
   *
   * <p>Attention, this method should be called in transaction. What's more, we should also consider
   * the concurrency issue. For example, if two threads want to create a new id for the same name,
   * the result is not deterministic if we do not use synchronization.
   */
  private synchronized long getOrCreateId(String name) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    byte[] idByte = backend.get(nameByte);
    if (idByte != null) {
      return ByteUtils.byteToLong(idByte);
    }

    byte[] maxByte = backend.get(NEXT_USABLE_ID);
    long id = maxByte == null ? 0 : ByteUtils.byteToLong(maxByte) + 1;
    maxByte = ByteUtils.longToByte(id);

    // Write current max id to storage
    backend.put(NEXT_USABLE_ID, maxByte, true);

    // Write name_metalake1 -> 1
    backend.put(nameByte, maxByte, false);

    // Write id_1 ---> metalake1
    byte[] idToNameKey = Bytes.concat(ID_PREFIX, maxByte);
    byte[] idToNameValue = name.getBytes();
    backend.put(idToNameKey, idToNameValue, false);
    return id;
  }

  /**
   * Encode entity key for KV backend, e.g., RocksDB. The key is used to store the entity in the
   * backend.
   *
   * @param entityType type of entity
   * @param identifier the name identifier of entity
   * @return the encoded key for key-value storage
   */
  private byte[] encodeEntity(NameIdentifier identifier, EntityType entityType) throws IOException {
    String[] nameSpace = identifier.namespace().levels();
    String name = identifier.name();
    long[] nameSpaceIds = new long[nameSpace.length];
    for (int i = 0; i < nameSpace.length; i++) {
      nameSpaceIds[i] = getOrCreateId(nameSpace[i]);
    }
    long nameId = getOrCreateId(name);
    switch (entityType) {
      case METALAKE:
        return Bytes.concat(
            METALAKE.getShortName().getBytes(), NAMESPACE_SEPARATOR, ByteUtils.longToByte(nameId));
      case CATALOG:
        return Bytes.concat(
            CATALOG.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(nameSpaceIds[0]),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(nameId));
      case SCHEMA:
        return Bytes.concat(
            SCHEMA.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(nameSpaceIds[0]),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(nameSpaceIds[1]),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(nameId));
      case TABLE:
        long schemaId = getOrCreateId(identifier.namespace().lastLevel());
        long tableId = getOrCreateId(identifier.name());
        return Bytes.concat(
            TABLE.getShortName().getBytes(),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(schemaId),
            NAMESPACE_SEPARATOR,
            ByteUtils.longToByte(tableId));
      default:
        throw new UnsupportedOperationException("Currently not support entity type: " + entityType);
    }
  }

  /**
   * This method is similar to {@link #encodeEntity(NameIdentifier, EntityType)}, but it is used to
   * list(scan) entities in the backend. For example, We want to list all catalogs in the backend,
   * we can use the prefix "ca_" to scan
   *
   * @param namespace namespace of current entity
   * @param namespaceType type of entity under the current namespace to scan, if the namespace is
   *     catalog and type is schema, we will scan all schemas under the catalog
   * @return start key to scan
   */
  private byte[] encodeNamespace(Namespace namespace, EntityType namespaceType) throws IOException {
    // First, we try to get the id from backend
    String[] nameSpace = namespace.levels();
    if (nameSpace.length == 0) {
      // Would be like ml_
      return Bytes.concat(METALAKE.getShortName().getBytes(), NAMESPACE_SEPARATOR);
    }

    if (nameSpace.length == 1) {
      long metalakeId = getOrCreateId(nameSpace[0]);
      byte[] prefix = Bytes.concat(CATALOG.getShortName().getBytes(), NAMESPACE_SEPARATOR);
      // Would be like ca_{metalakeId}_
      return Bytes.concat(prefix, ByteUtils.longToByte(metalakeId), NAMESPACE_SEPARATOR);
    }

    if (namespaceType == SCHEMA) {
      long metalakeId = getOrCreateId(nameSpace[0]);
      long catalogId = getOrCreateId(nameSpace[1]);
      byte[] prefix = Bytes.concat(SCHEMA.getShortName().getBytes(), NAMESPACE_SEPARATOR);
      // Would be like sc_{metalakeId}_{catalogId}_
      return Bytes.concat(
          prefix,
          ByteUtils.longToByte(metalakeId),
          NAMESPACE_SEPARATOR,
          ByteUtils.longToByte(catalogId),
          NAMESPACE_SEPARATOR);
    }

    if (namespaceType == EntityType.TABLE) {
      long schemaId = getOrCreateId(nameSpace[2]);
      byte[] prefix = Bytes.concat(TABLE.getShortName().getBytes(), NAMESPACE_SEPARATOR);
      // Would be like ta_{schemaId}_
      return Bytes.concat(prefix, ByteUtils.longToByte(schemaId), NAMESPACE_SEPARATOR);
    }

    throw new UnsupportedOperationException(
        "Currently not support namespace type: " + namespaceType);
  }

  public long getNextUsableId() throws IOException {
    byte[] maxByte = backend.get(NEXT_USABLE_ID);
    return maxByte == null ? 0 : ByteUtils.byteToLong(maxByte) + 1;
  }

  @Override
  public byte[] encode(NameIdentifier identifier, EntityType type) throws IOException {
    return encodeEntity(identifier, type);
  }

  @Override
  public byte[] encode(Namespace namespace, EntityType type) throws IOException {
    return encodeNamespace(namespace, type);
  }
}
