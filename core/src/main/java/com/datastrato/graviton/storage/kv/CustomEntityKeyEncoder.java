/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Entity.EntityType.CATALOG;
import static com.datastrato.graviton.Entity.EntityType.METALAKE;
import static com.datastrato.graviton.Entity.EntityType.SCHEMA;
import static com.datastrato.graviton.Entity.EntityType.TABLE;

import com.datastrato.graviton.Entity.EntityIdentifer;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.util.ByteUtils;
import com.datastrato.graviton.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;

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
 * ta_{ml_id}_{ca_id}_{sc_id}_{table_id}    ----->    table_info
 * ta_{ml_id}_{ca_id}_{sc_id}_{table_id}    ----->    table_info
 * to_{ml_id}_{ca_id}_{br_id}_{to_id}       ----->    topic_info
 * to_{ml_id}_{ca_id}_{br_id}_{to_id}       ----->    topic_info
 * </pre>
 */
public class CustomEntityKeyEncoder implements EntityKeyEncoder {
  public static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CustomEntityKeyEncoder.class);

  // name prefix of name in name to id mapping,
  // e.g., name_metalake1 -> 1
  //       name_metalake2 -> 2
  private static final byte[] NAME_PREFIX = "name_".getBytes();

  // id prefix of id in name to id mapping,
  // e.g., id_1 -> metalake1
  //       id_2 -> metalake2
  private static final byte[] ID_PREFIX = "id_".getBytes();

  // Store current max id, -1 if not exists, e.g., if current_max_id is 0, that means only one
  // ID has been created, and the next id should be 1.
  private static final byte[] CURRENT_MAX_ID = "current_max_id".getBytes();

  @VisibleForTesting static final byte[] NAMESPACE_SEPARATOR = "_".getBytes();

  private static final String WILD_CARD = "*";

  private final KvBackend backend;

  // This is for range query, e.g., get all metalakes, get all catalogs in a metalake, etc.
  public static final Map<EntityType, String> ENTITY_TYPE_TO_NAMESPACE =
      ImmutableMap.of(
          METALAKE, METALAKE.getShortName() + "_",
          CATALOG, CATALOG.getShortName() + "_{}_",
          SCHEMA, SCHEMA.getShortName() + "_{}_{}_",
          TABLE, TABLE.getShortName() + "_{}_{}_{}_");

  // This is for point query. E.g., get a specific metalake, get a specific catalog in a metalake,
  // etc.
  public static final Map<EntityType, String> ENTITY_TYPE_TO_NAME_IDENTIFIER =
      ImmutableMap.of(
          METALAKE, METALAKE.getShortName() + "_{}",
          CATALOG, CATALOG.getShortName() + "_{}_{}",
          SCHEMA, SCHEMA.getShortName() + "_{}_{}_{}",
          TABLE, TABLE.getShortName() + "_{}_{}_{}_{}");

  public CustomEntityKeyEncoder(KvBackend backend) {
    this.backend = backend;
  }

  /**
   * Get or create id for name. If the name is not in the storage, create a new id for it. The id is
   * the current max id + 1.
   *
   * <p>Attention, this method should be called in transaction. What's more, we should also consider
   * the concurrency issue. For example, if two threads want to create a new id for the same name,
   * the result is not deterministic if we do not use synchronization.
   */
  @VisibleForTesting
  synchronized long getOrCreateId(String name, boolean isMetalake) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    byte[] idByte = backend.get(nameByte);
    if (idByte != null) {
      return ByteUtils.byteToLong(idByte);
    }

    // Matalake use UUID as id, other entities use incremental id
    long id =
        isMetalake
            ? (UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE)
            : getNextUsableId();
    byte[] maxByte = ByteUtils.longToByte(id);
    LOG.info("Create new id '{}' for name '{}', isMetaLake '{}'", id, name, isMetalake);

    // Write current max id to storage
    if (!isMetalake) {
      backend.put(CURRENT_MAX_ID, maxByte, true);
    }

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
   * @param entityIdentifer the entity identifier to encode
   * @return the encoded key for key-value storage
   */
  private byte[] encodeEntity(EntityIdentifer entityIdentifer) throws IOException {
    EntityType entityType = entityIdentifer.getEntityType();
    String[] nameSpace = entityIdentifer.getNameIdentifier().namespace().levels();
    long[] namespaceIds = new long[nameSpace.length];
    for (int i = 0; i < nameSpace.length; i++) {
      namespaceIds[i] = getOrCreateId(nameSpace[i], i == 0);
    }
    NameIdentifier identifier = entityIdentifer.getNameIdentifier();
    // If the name is a wild card, we only need to encode the namespace.
    if (WILD_CARD.equals(identifier.name())) {
      String namespaceTemplate = ENTITY_TYPE_TO_NAMESPACE.get(entityType);
      if (namespaceTemplate == null) {
        throw new UnsupportedOperationException("Unsupported entity type: " + entityType);
      }
      return formatNamespaceTemplateToByte(namespaceTemplate, namespaceIds);
    }

    long[] fullIds = new long[namespaceIds.length + 1];
    System.arraycopy(namespaceIds, 0, fullIds, 0, namespaceIds.length);
    fullIds[namespaceIds.length] = getOrCreateId(identifier.name(), namespaceIds.length == 0);

    String nameIdentifierTemplate = ENTITY_TYPE_TO_NAME_IDENTIFIER.get(entityType);
    if (nameIdentifierTemplate == null) {
      throw new UnsupportedOperationException("Unsupported entity type: " + entityType);
    }
    return formatNameIdentiferTemplateToByte(nameIdentifierTemplate, fullIds);
  }

  /**
   * Format the name space template to a byte array. For example, if the name space template is
   * "ca_{}_{}" and the ids is [1, 2], the result is "ca_1_2"
   *
   * @param namespaceTemptalte the name space template, please see {@link #ENTITY_TYPE_TO_NAMESPACE}
   * @param ids the ids that namespace names map to
   */
  private byte[] formatNamespaceTemplateToByte(String namespaceTemptalte, long[] ids) {
    String[] parts = namespaceTemptalte.split("\\{\\}");
    Preconditions.checkArgument(parts.length == ids.length + 1);

    byte[] bytes = new byte[0];
    for (int i = 0; i < parts.length; i++) {
      if (i != parts.length - 1) {
        bytes = Bytes.concat(bytes, parts[i].getBytes(), ByteUtils.longToByte(ids[i]));
      } else {
        bytes = Bytes.concat(bytes, parts[i].getBytes());
      }
    }

    return bytes;
  }

  /**
   * Format the name identifer to a byte array. For example, if the name space template is
   * "ca_{}_{}" and the ids is [1, 2], the result is "ca_1_2"
   *
   * @param nameIdentierTemplate the name space template, please see {@link
   *     #ENTITY_TYPE_TO_NAMESPACE}
   * @param ids the ids that name identifier map to
   */
  private byte[] formatNameIdentiferTemplateToByte(String nameIdentierTemplate, long[] ids) {
    String[] parts = nameIdentierTemplate.split("\\{\\}");
    Preconditions.checkArgument(parts.length == ids.length);

    byte[] bytes = new byte[0];
    for (int i = 0; i < ids.length; i++) {
      bytes = Bytes.concat(bytes, parts[i].getBytes(), ByteUtils.longToByte(ids[i]));
    }

    return bytes;
  }

  @VisibleForTesting
  public long getNextUsableId() throws IOException {
    byte[] maxByte = backend.get(CURRENT_MAX_ID);
    return maxByte == null ? 0 : ByteUtils.byteToLong(maxByte) + 1;
  }

  @Override
  public byte[] encode(EntityIdentifer entityIdentifer) throws IOException {
    return encodeEntity(entityIdentifer);
  }
}
