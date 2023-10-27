/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Entity.EntityType.CATALOG;
import static com.datastrato.gravitino.Entity.EntityType.METALAKE;
import static com.datastrato.gravitino.Entity.EntityType.SCHEMA;
import static com.datastrato.gravitino.Entity.EntityType.TABLE;

import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.storage.NameMappingService;
import com.datastrato.gravitino.utils.ByteUtils;
import com.datastrato.gravitino.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encode entity key for KV backend, e.g., RocksDB. The key is used to store the entity in the
 * backend. The final key will be:
 *
 * <pre>
 *     Key                                Value
 * ml_{ml_id}               -----    metalake info
 * ml_{ml_id}               -----    metalake info
 * ca_{ml_id}_{ca_id}       -----    catalog_info
 * ca_{ml_id}_{ca_id}       -----    catalog_info
 * sc_{ml_id}_{ca_id}_{sc_id} ---    schema_info
 * sc_{ml_id}_{ca_id}_{sc_id} ---    schema_info
 * br_{ml_id}_{ca_id}_{br_id} ---    broker_info
 * br_{ml_id}_{ca_id}_{br_id} ---    broker_info
 *
 * ta_{ml_id}_{ca_id}_{sc_id}_{table_id}    -----    table_info
 * ta_{ml_id}_{ca_id}_{sc_id}_{table_id}    -----    table_info
 * to_{ml_id}_{ca_id}_{br_id}_{to_id}       -----    topic_info
 * to_{ml_id}_{ca_id}_{br_id}_{to_id}       -----    topic_info
 * </pre>
 */
public class BinaryEntityKeyEncoder implements KvEntityKeyEncoder {
  public static final Logger LOG = LoggerFactory.getLogger(BinaryEntityKeyEncoder.class);

  public static final String NAMESPACE_SEPARATOR = "/";

  @VisibleForTesting
  static final byte[] NAMESPACE_SEPARATOR_BYTE_ARRAY = NAMESPACE_SEPARATOR.getBytes();

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

  private String generateMappingKey(long[] namespaceIds, String name) {
    String context =
        Joiner.on(NAMESPACE_SEPARATOR)
            .join(
                Arrays.stream(namespaceIds).mapToObj(String::valueOf).collect(Collectors.toList()));
    return StringUtils.isBlank(context) ? name : context + NAMESPACE_SEPARATOR + name;
  }

  /**
   * Encode entity key for KV backend, e.g., RocksDB. The key is used to store the entity in the
   * backend.
   *
   * @param identifier NameIdentifier of the entity
   * @param entityType the entity identifier to encode
   * @param nullIfMissing return null if name-id mapping does not contain the mapping of identifier
   * @return the encoded key for key-value storage. null if returnIfEntityNotFound is true and the
   *     entity the identifier represents does not exist;
   */
  private byte[] encodeEntity(
      NameIdentifier identifier, EntityType entityType, boolean nullIfMissing) throws IOException {
    String[] nameSpace = identifier.namespace().levels();
    long[] namespaceIds = new long[nameSpace.length];
    for (int i = 0; i < nameSpace.length; i++) {
      String nameKey = generateMappingKey(ArrayUtils.subarray(namespaceIds, 0, i), nameSpace[i]);
      if (nullIfMissing && null == nameMappingService.getIdByName(nameKey)) {
        return null;
      }

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
    String nameKey = generateMappingKey(namespaceIds, identifier.name());
    if (nullIfMissing && null == nameMappingService.getIdByName(nameKey)) {
      return null;
    }

    long[] namespaceAndNameIds =
        ArrayUtils.add(namespaceIds, nameMappingService.getOrCreateIdFromName(nameKey));

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
   * @param nullIfMissing return null if the entity with the name 'ident' not found
   * @return The byte array representing the encoded key.
   */
  @Override
  public byte[] encode(NameIdentifier ident, EntityType type, boolean nullIfMissing)
      throws IOException {
    return encodeEntity(ident, type, nullIfMissing);
  }

  @Override
  public List<byte[]> getParentPrefix(NameIdentifier ident, EntityType entityType)
      throws IOException {
    List<byte[]> prefixs = Lists.newArrayList();
    byte[] encode = encode(ident, entityType, true);
    if (encode == null) {
      return prefixs;
    }

    switch (entityType) {
      case TABLE:
        prefixs.add(encode(ident.parent(), SCHEMA));
        prefixs.add(encode(ident.parent().parent(), CATALOG));
        prefixs.add(encode(ident.parent().parent().parent(), METALAKE));
        break;
      case SCHEMA:
        prefixs.add(encode(ident.parent(), CATALOG));
        prefixs.add(encode(ident.parent().parent(), METALAKE));
        break;
      case CATALOG:
        prefixs.add(encode(ident.parent(), METALAKE));
        break;
      case METALAKE:
        break;
      default:
        LOG.warn("Currently unknown type: {}, please check it", entityType);
    }

    return prefixs;
  }

  /**
   * Get key prefix of all sub-entities under a specific entities. For example, as a metalake will
   * start with `ml_{metalake_id}`, sub-entities under this metalake will have the prefix
   *
   * <pre>
   *   catalog: ca_{metalake_id}
   *   schema:  sc_{metalake_id}
   *   table:   ta_{metalake_id}
   * </pre>
   *
   * Why the sub-entities under this metalake start with those prefixes, please see {@link
   * KvEntityStore} java class doc.
   *
   * @param ident identifier of an entity
   * @param entityType type of entity
   * @return list of sub-entities prefix
   * @throws IOException if error occurs
   */
  @Override
  public List<byte[]> getChildrenPrefix(NameIdentifier ident, EntityType entityType)
      throws IOException {
    List<byte[]> prefixs = Lists.newArrayList();
    byte[] encode = encode(ident, entityType, false);
    switch (entityType) {
      case METALAKE:
        prefixs.add(replacePrefixTypeInfo(encode, CATALOG.getShortName()));
      case CATALOG:
        prefixs.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
      case SCHEMA:
        prefixs.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
      case TABLE:
        break;
      default:
        LOG.warn("Currently unknown type: {}, please check it", entityType);
    }
    Collections.reverse(prefixs);
    return prefixs;
  }

  private byte[] replacePrefixTypeInfo(byte[] encode, String subTypePrefix) {
    byte[] result = new byte[encode.length];
    System.arraycopy(encode, 0, result, 0, encode.length);
    byte[] bytes = subTypePrefix.getBytes();
    result[0] = bytes[0];
    result[1] = bytes[1];

    return result;
  }

  @Override
  public Pair<NameIdentifier, EntityType> decode(byte[] key) throws IOException {
    // Check the first two bytes to get the entity type
    byte[] prefix = ArrayUtils.subarray(key, 0, 2);
    String typeName = new String(prefix);
    EntityType entityType = EntityType.fromShortName(typeName);
    // Then get the namespace
    byte[] namespace = ArrayUtils.subarray(key, 3, key.length);
    NameIdentifier identifier = decodeIdentifier(namespace);
    return Pair.of(identifier, entityType);
  }

  private NameIdentifier decodeIdentifier(byte[] namespaceArray) throws IOException {
    int start = 0;
    List<String> namespaces = Lists.newArrayList();
    while (start < namespaceArray.length) {
      long id = ByteUtils.byteToLong(ArrayUtils.subarray(namespaceArray, start, start + 8));
      String nameWithContext = nameMappingService.getNameById(id);
      if (nameWithContext == null) {
        throw new IllegalStateException("Cannot find name for id: " + id);
      }

      namespaces.add(
          Iterables.getLast(Splitter.on(NAMESPACE_SEPARATOR).splitToList(nameWithContext)));
      start += 9;
    }
    return NameIdentifier.of(namespaces.toArray(new String[0]));
  }
}
