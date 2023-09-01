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
import com.datastrato.graviton.storage.NameMappingService;
import com.datastrato.graviton.utils.ByteUtils;
import com.datastrato.graviton.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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

  public static final String NAMESPACE_SEPARATOR = "/";

  @VisibleForTesting
  static final byte[] BYTABLE_NAMESPACE_SEPARATOR = NAMESPACE_SEPARATOR.getBytes();

  static final String WILD_CARD = "*";

  // Key format template. Please the comment of the class for more details.
  public static final Map<EntityType, String[]> ENTITY_TYPE_TO_NAME_IDENTIFIER =
      ImmutableMap.of(
          METALAKE, new String[] {METALAKE.getShortName() + "/"},
          CATALOG, new String[] {CATALOG.getShortName() + "/", "/"},
          SCHEMA, new String[] {SCHEMA.getShortName() + "/", "/", "/"},
          TABLE, new String[] {TABLE.getShortName() + "/", "/", "/", "/"});

  @VisibleForTesting final NameMappingService nameMappingService;

  public BinaryEntityKeyEncoder(NameMappingService nameMappingService) {
    this.nameMappingService = nameMappingService;
  }

  private String nameContext(long[] namespace) {
    return Joiner.on(NAMESPACE_SEPARATOR)
        .join(Arrays.stream(namespace).mapToObj(String::valueOf).collect(Collectors.toList()));
  }

  /**
   * Generate the key for name to id mapping. Currently, the mapping is as following.
   *
   * <pre>
   *   Assume we have the following entities:
   *   metalake: a1        ----> 1
   *   catalog : a1.b1     ----> 2
   *   schema  : a1.b1.c   ----> 3
   *
   *   metalake: a2        ----> 4
   *   catalog : a2.b2     ----> 5
   *   schema  : a2.b2.c   ----> 6
   *   schema  : a2.b2.c1  ----> 7
   *
   *   metalake: a1        ----> 1 means the name of metalake is a1 and the corresponding id is 1
   * </pre>
   *
   * Then we will store the name to id mapping as follows
   *
   * <pre>
   *  a1         --> 1
   * 	1/b1       --> 2
   * 	1/2/c      --> 3
   * 	a2         --> 4
   * 	4/b2       --> 5
   * 	4/5/c      --> 6
   * 	4/5/c1     --> 7
   * </pre>
   *
   * @param namespaceIds namespace of a specific entity
   * @param name name of a specific entity
   * @return key that maps to the id of a specific entity
   */
  private String generateKeyForNameMapping(long[] namespaceIds, String name) {
    String context = nameContext(namespaceIds);
    return StringUtils.isBlank(context) ? name : context + NAMESPACE_SEPARATOR + name;
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
      String nameKey =
          generateKeyForNameMapping(ArrayUtils.subarray(namespaceIds, 0, i), nameSpace[i]);
      namespaceIds[i] = nameMappingService.getOrCreateIdFromName(nameKey);
    }

    // If the name is a wildcard, We only need to encode the namespace.
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
    String nameKey = generateKeyForNameMapping(namespaceIds, identifier.name());
    namespaceAndNameIds[namespaceIds.length] = nameMappingService.getOrCreateIdFromName(nameKey);

    String[] nameIdentifierTemplate = ENTITY_TYPE_TO_NAME_IDENTIFIER.get(entityType);
    if (nameIdentifierTemplate == null) {
      throw new UnsupportedOperationException("Unsupported entity type: " + entityType);
    }
    return formatNameIdentifierTemplateToByte(nameIdentifierTemplate, namespaceAndNameIds);
  }

  /**
   * Format the name space template to a byte array. For example, if the name space template is
   * "ca/{}/" and the ids are [1], the result is "ca/1/" which means we want to get all catalogs in
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
   * "ca/{}/{}" and the ids is [1, 2], the result is "ca/1/2" which means we want to get the
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
