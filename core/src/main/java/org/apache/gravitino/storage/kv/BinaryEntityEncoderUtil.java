/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.kv;

import static org.apache.gravitino.Entity.EntityType.CATALOG;
import static org.apache.gravitino.Entity.EntityType.FILESET;
import static org.apache.gravitino.Entity.EntityType.METALAKE;
import static org.apache.gravitino.Entity.EntityType.SCHEMA;
import static org.apache.gravitino.Entity.EntityType.TABLE;
import static org.apache.gravitino.Entity.EntityType.TOPIC;
import static org.apache.gravitino.storage.kv.BinaryEntityKeyEncoder.LOG;
import static org.apache.gravitino.storage.kv.BinaryEntityKeyEncoder.NAMESPACE_SEPARATOR;
import static org.apache.gravitino.storage.kv.BinaryEntityKeyEncoder.TYPE_AND_NAME_SEPARATOR;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.storage.NameMappingService;

public class BinaryEntityEncoderUtil {

  // The entity types in version 0.4.x, entities in this set do not have the prefix in the name-id
  // mapping. Why do we introduce it? We need to make it backward compatible.
  public static final Set<EntityType> VERSION_0_4_COMPATIBLE_ENTITY_TYPES =
      ImmutableSet.of(METALAKE, CATALOG, SCHEMA, TABLE);

  private BinaryEntityEncoderUtil() {}

  /**
   * Generate the key for name to id mapping. Currently, the mapping is as following.
   *
   * <pre>
   *   Assume we have the following entities:
   *   metalake: a1        ---- 1
   *   catalog : a1.b1     ---- 2
   *   schema  : a1.b1.c   ---- 3
   *
   *   metalake: a2        ---- 4
   *   catalog : a2.b2     ---- 5
   *   schema  : a2.b2.c   ---- 6
   *   schema  : a2.b2.c1  ---- 7
   *
   *   metalake: a1        ---- 1 means the name of metalake is a1 and the corresponding id is 1
   * </pre>
   *
   * Then we will store the name to id mapping as follows
   *
   * <pre>
   *  a1         -- 1
   * 	1/b1       -- 2
   * 	1/2/c      -- 3
   * 	a2         -- 4
   * 	4/b2       -- 5
   * 	4/5/c      -- 6
   * 	4/5/c1     -- 7
   * </pre>
   *
   * @param nameIdentifier name of a specific entity
   * @return key that maps to the id of a specific entity. See above, The key maybe like '4/5/c1'
   * @throws IOException if error occurs
   */
  public static String generateKeyForMapping(
      NameIdentifier nameIdentifier, EntityType entityType, NameMappingService nameMappingService)
      throws IOException {
    Namespace namespace = nameIdentifier.namespace();
    String name = nameIdentifier.name();

    List<EntityType> parentTypes = EntityType.getParentEntityTypes(entityType);
    long[] ids = new long[namespace.length()];
    for (int i = 0; i < ids.length; i++) {
      ids[i] =
          nameMappingService.getIdByName(
              concatIdAndName(
                  ArrayUtils.subarray(ids, 0, i), namespace.level(i), parentTypes.get(i)));
    }

    return concatIdAndName(ids, name, entityType);
  }

  /**
   * Concatenate the namespace ids and the name of the entity. Assuming the namespace ids are [1, 2]
   * and the name is 'schema', the result will be '1/2/sc_schema'.
   *
   * <p>Attention, in order to make this change backward compatible, if the entity type is TABLE, we
   * will not add a prefix to the name. If the entity type is not TABLE, we will add the prefix to
   * the name.
   *
   * @param namespaceIds namespace ids, which are the ids of the parent entities
   * @param name name of the entity
   * @param type type of the entity
   * @return concatenated string that used in the id-name mapping.
   */
  public static String concatIdAndName(long[] namespaceIds, String name, EntityType type) {
    String context =
        Joiner.on(NAMESPACE_SEPARATOR)
            .join(
                Arrays.stream(namespaceIds).mapToObj(String::valueOf).collect(Collectors.toList()));
    // We need to make it backward compatible, so we need to check if the name is already prefixed.
    String mappingName =
        VERSION_0_4_COMPATIBLE_ENTITY_TYPES.contains(type)
            ? name
            : type.getShortName() + TYPE_AND_NAME_SEPARATOR + name;
    return StringUtils.isBlank(context) ? mappingName : context + NAMESPACE_SEPARATOR + mappingName;
  }

  /**
   * Get key prefix of all sub-entities under a specific entities. For example, as a metalake will
   * start with `ml/{metalake_id}`, sub-entities under this metalake will have the prefix
   *
   * <pre>
   *   catalog: ca/{metalake_id}
   *   schema:  sc/{metalake_id}
   *   table:   ta/{metalake_id}
   * </pre>
   *
   * Why the sub-entities under this metalake start with those prefixes, please see {@link
   * KvEntityStore} java class doc.
   *
   * @param ident identifier of an entity
   * @param type type of entity
   * @return list of sub-entities prefix
   * @throws IOException if error occurs
   */
  public static List<byte[]> getSubEntitiesPrefix(
      NameIdentifier ident, EntityType type, BinaryEntityKeyEncoder entityKeyEncoder)
      throws IOException {
    List<byte[]> prefixes = Lists.newArrayList();
    byte[] encode = entityKeyEncoder.encode(ident, type, true);
    switch (type) {
      case METALAKE:
        prefixes.add(replacePrefixTypeInfo(encode, CATALOG.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, FILESET.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TOPIC.getShortName()));
        break;
      case CATALOG:
        prefixes.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, FILESET.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TOPIC.getShortName()));
        break;
      case SCHEMA:
        prefixes.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, FILESET.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TOPIC.getShortName()));
        break;
      case TABLE:
      case FILESET:
      case TOPIC:
        break;
      default:
        LOG.warn("Currently unknown type: {}, please check it", type);
    }
    Collections.reverse(prefixes);
    return prefixes;
  }

  /**
   * Replace the prefix type info with the new type info.
   *
   * @param encode the encoded byte array
   * @param subTypePrefix the new type prefix
   * @return the new byte array
   */
  public static byte[] replacePrefixTypeInfo(byte[] encode, String subTypePrefix) {
    byte[] result = new byte[encode.length];
    System.arraycopy(encode, 0, result, 0, encode.length);
    byte[] bytes = subTypePrefix.getBytes(StandardCharsets.UTF_8);
    result[0] = bytes[0];
    result[1] = bytes[1];

    return result;
  }
}
