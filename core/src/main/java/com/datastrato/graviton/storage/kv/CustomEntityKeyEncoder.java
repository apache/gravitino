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
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.storage.InMemoryNameMappingService;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.util.ByteUtils;
import com.datastrato.graviton.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final Logger LOG = LoggerFactory.getLogger(CustomEntityKeyEncoder.class);

  // TODO(yuqi) should be configuratable;
  private static final NameMappingService NAME_MAPPING_SERVICE =
      InMemoryNameMappingService.INSTANCE;

  @Override
  public byte[] encode(NameIdentifier identifier, EntityType type, boolean createIdIfNotExists)
      throws IOException {
    // TODO(yuqi)
    // Using NAME_MAPPING_SERVICE to implement EntityKeyEncoder without using KvBackend directly.
    return new byte[0];
  }

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

  @VisibleForTesting static final String WILD_CARD = "*";

  private final KvBackend backend;

  // Key format template. Please the comment of the class for more details.
  public static final Map<EntityType, String[]> ENTITY_TYPE_TO_NAME_IDENTIFIER =
      ImmutableMap.of(
          METALAKE, new String[] {METALAKE.getShortName() + "_"},
          CATALOG, new String[] {CATALOG.getShortName() + "_", "_"},
          SCHEMA, new String[] {SCHEMA.getShortName() + "_", "_", "_"},
          TABLE, new String[] {TABLE.getShortName() + "_", "_", "_", "_"});

  public CustomEntityKeyEncoder(KvBackend backend) {
    this.backend = backend;
  }

  /**
   * Get or create id for name. If the name is not in the storage, create a new id for it. The id is
   * the current max id + 1. Matalake and catalog will directly use UUID as id, other entities use
   * incremental id
   *
   * <p>Attention, this method should be called in transaction. What's more, we should also consider
   * the concurrency issue. For example, if two threads want to create a new id for the same name,
   * the result is not deterministic if we do not use synchronization.
   */
  @VisibleForTesting
  synchronized long getOrCreateId(
      String name, boolean isMetalakeOrCatalog, boolean createIdIfNotExists) throws IOException {
    byte[] nameByte = Bytes.concat(NAME_PREFIX, name.getBytes());
    byte[] idByte = backend.get(nameByte);
    if (idByte != null) {
      return ByteUtils.byteToLong(idByte);
    }

    // Create if not exists
    if (!createIdIfNotExists) {
      throw new NoSuchEntityException("No such entity with name " + name);
    }

    // Now we need to create a new id for the name, So if this is not in transaction, throw an
    // Exception
    if (!backend.isInTransaction()) {
      throw new IOException("getOrCreateId should be called in transaction");
    }

    // Matalake and catalog use UUID as id, other entities use incremental id
    // According to
    // https://stackoverflow.com/questions/325443/likelihood-of-collision-using-most-significant-bits-of-a-uuid-in-java
    // UUID.randomUUID().getLeastSignificantBits() is a good choice for id
    long id = isMetalakeOrCatalog ? UUID.randomUUID().getLeastSignificantBits() : getNextUsableId();
    byte[] maxByte = ByteUtils.longToByte(id);
    LOG.info(
        "Create new id '{}' for name '{}',  isMetalakeOrCatalog '{}'",
        id,
        name,
        isMetalakeOrCatalog);

    // Write current max id to storage, For metalake as it's generated from UUID, No need to store
    // it
    if (!isMetalakeOrCatalog) {
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
   * @param entityIdentifier the entity identifier to encode
   * @return the encoded key for key-value storage
   */
  private byte[] encodeEntity(EntityIdentifer entityIdentifier, boolean createIdIfNotExists)
      throws IOException {
    EntityType entityType = entityIdentifier.getEntityType();
    String[] nameSpace = entityIdentifier.getNameIdentifier().namespace().levels();
    long[] namespaceIds = new long[nameSpace.length];
    for (int i = 0; i < nameSpace.length; i++) {
      namespaceIds[i] =
          getOrCreateId(nameSpace[i], i <= 1 /* is metalake or catalog */, createIdIfNotExists);
    }

    NameIdentifier identifier = entityIdentifier.getNameIdentifier();
    // If the name is a wild card, we only need to encode the namespace.
    if (WILD_CARD.equals(identifier.name())) {
      String[] namespaceTemplate = ENTITY_TYPE_TO_NAME_IDENTIFIER.get(entityType);
      if (namespaceTemplate == null) {
        throw new UnsupportedOperationException("Unsupported entity type: " + entityType);
      }
      return formatNamespaceTemplateToByte(namespaceTemplate, namespaceIds);
    }

    // This is for point query and need to use specific name
    long[] namespaceAndNameIds = new long[namespaceIds.length + 1];
    System.arraycopy(namespaceIds, 0, namespaceAndNameIds, 0, namespaceIds.length);
    namespaceAndNameIds[namespaceIds.length] =
        getOrCreateId(identifier.name(), namespaceIds.length <= 1, createIdIfNotExists);

    String[] nameIdentifierTemplate = ENTITY_TYPE_TO_NAME_IDENTIFIER.get(entityType);
    if (nameIdentifierTemplate == null) {
      throw new UnsupportedOperationException("Unsupported entity type: " + entityType);
    }
    return formatNameIdentifierTemplateToByte(nameIdentifierTemplate, namespaceAndNameIds);
  }

  /**
   * Format the name space template to a byte array. For example, if the name space template is
   * "ca_{}_" and the ids are [1], the result is "ca_1_" which means we want to get all catalogs in
   * metalake '1'
   *
   * @param namespaceTemplate the name space template, please see {@link
   *     #ENTITY_TYPE_TO_NAME_IDENTIFIER}
   * @param ids the ids that namespace names map to
   */
  private byte[] formatNamespaceTemplateToByte(String[] namespaceTemplate, long[] ids) {
    Preconditions.checkArgument(namespaceTemplate.length == ids.length + 1);

    byte[] bytes = new byte[0];
    for (int i = 0; i < namespaceTemplate.length; i++) {
      if (i != namespaceTemplate.length - 1) {
        bytes = Bytes.concat(bytes, namespaceTemplate[i].getBytes(), ByteUtils.longToByte(ids[i]));
      } else {
        bytes = Bytes.concat(bytes, namespaceTemplate[i].getBytes());
      }
    }

    return bytes;
  }

  /**
   * Format the name identifier to a byte array. For example, if the name space template is
   * "ca_{}_{}" and the ids is [1, 2], the result is "ca_1_2" which means we want to get the
   * specific catalog '2'
   *
   * @param nameIdentifierTemplate the name space template, please see {@link
   *     #ENTITY_TYPE_TO_NAME_IDENTIFIER}
   * @param ids the ids that name identifier map to
   */
  private byte[] formatNameIdentifierTemplateToByte(String[] nameIdentifierTemplate, long[] ids) {
    Preconditions.checkArgument(nameIdentifierTemplate.length == ids.length);

    byte[] bytes = new byte[0];
    for (int i = 0; i < ids.length; i++) {
      bytes =
          Bytes.concat(bytes, nameIdentifierTemplate[i].getBytes(), ByteUtils.longToByte(ids[i]));
    }

    return bytes;
  }

  @VisibleForTesting
  public long getNextUsableId() throws IOException {
    byte[] maxByte = backend.get(CURRENT_MAX_ID);
    return maxByte == null ? 0 : ByteUtils.byteToLong(maxByte) + 1;
  }

  @Override
  public byte[] encode(EntityIdentifer entityIdentifer, boolean createIdIfNotExists)
      throws IOException {
    return encodeEntity(entityIdentifer, createIdIfNotExists);
  }
}
