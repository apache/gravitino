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
package org.apache.gravitino.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

/**
 * Key layout of {@link RedisEntityCache}. Every key derives from {@link EntityCacheKey#toString()},
 * which is {@code <identifier>:<TYPE>}, for example {@code m1.c1.s1.t1:TABLE}.
 *
 * <table>
 *   <caption>Keys written under one namespace</caption>
 *   <tr><th>Purpose</th><th>Key</th><th>Redis type</th></tr>
 *   <tr><td>Value</td><td>{@code <ns>:{<metalake>}:D:<identifier>:<TYPE>}</td><td>string</td></tr>
 *   <tr><td>Fence</td><td>{@code <ns>:{<metalake>}:F:<identifier>}</td><td>string (counter)</td></tr>
 *   <tr><td>Index</td><td>{@code <ns>:{<metalake>}:IDX}</td><td>sorted set, all scores 0</td></tr>
 * </table>
 *
 * <p>The {@code {<metalake>}} segment is a Redis Cluster hash tag holding the first level of the
 * identifier, so every key of one metalake lives in one hash slot and the multi-key scripts that
 * drop a container and its descendants never span slots. The index holds one member per cached
 * value, {@code <identifier>:<TYPE>}, so the descendants of a container are exactly the members in
 * the lexicographic range starting with {@code <identifier>.} (and, for a schema, {@code
 * <identifier><schema separator>}).
 *
 * <p>A fence is a counter keyed by the identifier alone, without the type. A container drop
 * increments the fence of the dropped identifier; a write compares the fences of the identifier and
 * of every ancestor against the values it observed before it loaded the entity, so a load that
 * began before a drop anywhere above it cannot refill the key afterwards.
 */
final class RedisKeyspace {

  /** Separates {@link NameIdentifier} levels inside a key. */
  static final String NAME_LEVEL_BOUNDARY = ".";

  /** Index of the schema level in a fully qualified identifier: metalake, catalog, schema. */
  private static final int SCHEMA_LEVEL = 2;

  private static final String VALUE_MARKER = "D:";
  private static final String FENCE_MARKER = "F:";
  private static final String INDEX_NAME = "IDX";

  private final String namespace;

  RedisKeyspace(String namespace) {
    Preconditions.checkArgument(StringUtils.isNotBlank(namespace), "namespace must not be blank");
    Preconditions.checkArgument(
        !namespace.contains("{") && !namespace.contains("}"),
        "namespace must not contain '{' or '}'");
    this.namespace = namespace;
  }

  /** The hash tag of an identifier: the metalake, which is its first level. */
  static String hashTag(NameIdentifier ident) {
    return ident.hasNamespace() ? ident.namespace().level(0) : ident.name();
  }

  /** The index member for a key, {@code <identifier>:<TYPE>}. */
  static String member(EntityCacheKey key) {
    return key.toString();
  }

  /** Prefix shared by every key of the identifier's metalake: {@code <ns>:{<metalake>}:}. */
  String slotPrefix(NameIdentifier ident) {
    return namespace + ":{" + hashTag(ident) + "}:";
  }

  String indexKey(NameIdentifier ident) {
    return slotPrefix(ident) + INDEX_NAME;
  }

  String valueKey(EntityCacheKey key) {
    return slotPrefix(key.identifier()) + VALUE_MARKER + member(key);
  }

  /**
   * The fence key of one identifier path, see {@link #fencePaths(NameIdentifier, String)}.
   *
   * @param ident The identifier whose metalake selects the hash slot
   * @param identifierPath The fenced identifier, the entity's own or an ancestor's
   */
  String fenceKey(NameIdentifier ident, String identifierPath) {
    return slotPrefix(ident) + FENCE_MARKER + identifierPath;
  }

  /** Glob pattern matching every key this namespace writes, for {@code SCAN}. */
  String allKeysPattern() {
    return namespace + ":*";
  }

  /** Glob pattern matching every index key this namespace writes, for {@code SCAN}. */
  String allIndexKeysPattern() {
    return namespace + ":{*}:" + INDEX_NAME;
  }

  /** Whether a key returned by {@code SCAN} is a fence key. */
  static boolean isFenceKey(String key) {
    return key.contains("}:" + FENCE_MARKER);
  }

  /**
   * The identifier paths whose fences guard a write of the given identifier: every ancestor from
   * the metalake down, plus the identifier itself. A nested schema such as {@code raw:events:2024}
   * also contributes each of its hierarchical prefixes ({@code raw}, {@code raw:events}), because
   * dropping any of those drops the nested schema and everything under it.
   *
   * @param ident The identifier being written
   * @param schemaSeparator The configured hierarchical schema separator, may be blank
   * @return The fenced identifier paths, shallowest first, ending with the identifier itself
   */
  static List<String> fencePaths(NameIdentifier ident, String schemaSeparator) {
    Namespace ns = ident.namespace();
    List<String> paths = Lists.newArrayList();
    StringBuilder path = new StringBuilder();
    for (int level = 0; level <= ns.length(); level++) {
      String levelName = level < ns.length() ? ns.level(level) : ident.name();
      String parent = path.length() == 0 ? "" : path + NAME_LEVEL_BOUNDARY;
      if (level == SCHEMA_LEVEL
          && StringUtils.isNotEmpty(schemaSeparator)
          && levelName.contains(schemaSeparator)) {
        String[] parts = levelName.split(Pattern.quote(schemaSeparator), -1);
        StringBuilder nested = new StringBuilder(parent);
        for (int i = 0; i < parts.length - 1; i++) {
          nested.append(i == 0 ? "" : schemaSeparator).append(parts[i]);
          paths.add(nested.toString());
        }
      }
      path.setLength(0);
      path.append(parent).append(levelName);
      paths.add(path.toString());
    }
    return paths;
  }

  /**
   * The index-member prefixes whose entries a drop of the given key removes: the ordinary children
   * below {@code <identifier>.} and, for a schema, the nested schemas below {@code
   * <identifier><schema separator>}. Matching on a boundary rather than the bare identifier keeps
   * {@code catalog1} from matching {@code catalog10}.
   *
   * @param key The key being dropped
   * @param schemaSeparator The configured hierarchical schema separator, may be blank
   * @return The prefixes to range over in the index
   */
  static List<String> descendantPrefixes(EntityCacheKey key, String schemaSeparator) {
    String identifier = key.identifier().toString();
    List<String> prefixes = Lists.newArrayList(identifier + NAME_LEVEL_BOUNDARY);
    if (key.entityType() == Entity.EntityType.SCHEMA && StringUtils.isNotEmpty(schemaSeparator)) {
      prefixes.add(identifier + schemaSeparator);
    }
    return prefixes;
  }
}
