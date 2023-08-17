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
import com.datastrato.graviton.storage.EntityKeyEncoder;
import com.datastrato.graviton.storage.IdGenerator;
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.util.ByteUtils;
import com.datastrato.graviton.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
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
public class BinaryEntityKeyEncoder implements EntityKeyEncoder<byte[]> {
  public static final Logger LOG = LoggerFactory.getLogger(BinaryEntityKeyEncoder.class);

  @VisibleForTesting static final byte[] NAMESPACE_SEPARATOR = "_".getBytes();

  @VisibleForTesting static final String WILD_CARD = "*";

  // Key format template. Please the comment of the class for more details.
  public static final Map<EntityType, String[]> ENTITY_TYPE_TO_NAME_IDENTIFIER =
      ImmutableMap.of(
          METALAKE, new String[] {METALAKE.getShortName() + "_"},
          CATALOG, new String[] {CATALOG.getShortName() + "_", "_"},
          SCHEMA, new String[] {SCHEMA.getShortName() + "_", "_", "_"},
          TABLE, new String[] {TABLE.getShortName() + "_", "_", "_", "_"});

  @VisibleForTesting final IdGenerator idGenerator;
  private final NameMappingService nameMappingService;

  public BinaryEntityKeyEncoder(NameMappingService nameMappingService, IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
    this.nameMappingService = nameMappingService;
  }

  /**
   * Encode entity key for KV backend, e.g., RocksDB. The key is used to store the entity in the
   * backend.
   *
   * @param identifier NameIdentifier of the entity
   * @param entityType the entity identifier to encode
   * @return the encoded key for key-value storage
   */
  private byte[] encodeEntity(NameIdentifier identifier, EntityType entityType) throws IOException {
    String[] nameSpace = identifier.namespace().levels();
    long[] namespaceIds = new long[nameSpace.length];
    for (int i = 0; i < nameSpace.length; i++) {
      Long id = nameMappingService.getIdFromBinding(nameSpace[i]);
      if (id == null) {
        id = idGenerator.nextId();
        nameMappingService.addBinding(nameSpace[i], id);
      }
      namespaceIds[i] = id;
    }

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

    Long nameId = nameMappingService.getIdFromBinding(identifier.name());
    if (nameId == null) {
      nameId = idGenerator.nextId();
      nameMappingService.addBinding(identifier.name(), nameId);
    }
    namespaceAndNameIds[namespaceIds.length] = nameId;

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
  /**
   * Encodes an entity object into a byte array for use as a key in a key-value store.
   *
   * @param ident NameIdentifier of the entity
   * @param type the entity identifier to encode
   * @return The byte array representing the encoded key.
   */
  @Override
  public byte[] encode(NameIdentifier ident, EntityType type) throws IOException {
    return encodeEntity(ident, type);
  }
}
