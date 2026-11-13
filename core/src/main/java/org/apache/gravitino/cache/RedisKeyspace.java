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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

/**
 * Key layout of {@link RedisEntityCache}. Every key derives from {@link EntityCacheKey#toString()},
 * which is {@code <identifier>:<TYPE>}, for example {@code m1.c1.s1.t1:TABLE}.
 *
 * <table>
 *   <caption>Keys written under one namespace</caption>
 *   <tr><th>Purpose</th><th>Key</th><th>Redis type</th><th>TTL</th></tr>
 *   <tr><td>Value</td><td>{@code <ns>:{<metalake>}:D:<identifier>:<TYPE>}</td>
 *       <td>hash: {@code v} bytes, {@code s} stamp</td><td>the value TTL</td></tr>
 *   <tr><td>Fences</td><td>{@code <ns>:{<metalake>}:F}</td>
 *       <td>hash: identifier to generation</td><td>none</td></tr>
 *   <tr><td>Generation</td><td>{@code <ns>:{<metalake>}:G}</td><td>string (counter)</td>
 *       <td>none</td></tr>
 * </table>
 *
 * <p>The {@code {<metalake>}} segment is a Redis Cluster hash tag holding the first level of the
 * identifier, so every key of one metalake lives in one hash slot and every script here is
 * single-slot.
 *
 * <p>A fence is keyed by the identifier alone, without the type, and holds the generation at which
 * that identifier was last invalidated; an identifier never invalidated has no field and reads as
 * {@code 0}. Generations are drawn from the metalake's counter, so no two invalidations produce the
 * same value. The <em>stamp</em> of a key is the list of fences of the identifier and of every
 * ancestor (see {@link #fencePaths(NameIdentifier, String)}). A value carries the stamp it was
 * filled under and is served only while the stamp still matches, so invalidating a container moves
 * one fence and every descendant stops being served at once, without being enumerated.
 *
 * <p>Neither the fences nor the counter carry a TTL, so a {@code volatile-*} eviction policy never
 * removes them. An {@code allkeys-*} policy could, which is why the cache refuses to start under
 * one.
 *
 * <p>The namespace is restricted to letters, digits and {@code . _ - :} so that it can never carry
 * a hash tag or a glob metacharacter.
 */
final class RedisKeyspace {

  /** Separates {@link NameIdentifier} levels inside a key. */
  static final String NAME_LEVEL_BOUNDARY = ".";

  /** Characters a namespace may contain: no braces, no glob metacharacters, no whitespace. */
  static final Pattern NAMESPACE_PATTERN = Pattern.compile("[A-Za-z0-9._:-]+");

  /** Index of the schema level in a fully qualified identifier: metalake, catalog, schema. */
  private static final int SCHEMA_LEVEL = 2;

  private static final String VALUE_MARKER = "D:";
  private static final String FENCES_NAME = "F";
  private static final String GENERATION_NAME = "G";
  private static final String TAG_OPEN = ":{";
  private static final String TAG_CLOSE = "}:";

  private final String namespace;

  RedisKeyspace(String namespace) {
    Preconditions.checkArgument(
        namespace != null && NAMESPACE_PATTERN.matcher(namespace).matches(),
        "namespace must be non-empty and contain only letters, digits and '.', '_', '-', ':'");
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
    return slotPrefix(hashTag(ident));
  }

  /** Prefix shared by every key of the named metalake: {@code <ns>:{<metalake>}:}. */
  String slotPrefix(String metalake) {
    return namespace + TAG_OPEN + metalake + TAG_CLOSE;
  }

  String valueKey(EntityCacheKey key) {
    return slotPrefix(key.identifier()) + VALUE_MARKER + member(key);
  }

  /** The per-metalake generation counter every fence value is drawn from. Never expires. */
  String generationKey(NameIdentifier ident) {
    return generationKey(hashTag(ident));
  }

  String generationKey(String metalake) {
    return slotPrefix(metalake) + GENERATION_NAME;
  }

  /** The per-metalake hash of fences, identifier path to generation. Never expires. */
  String fencesKey(NameIdentifier ident) {
    return fencesKey(hashTag(ident));
  }

  String fencesKey(String metalake) {
    return slotPrefix(metalake) + FENCES_NAME;
  }

  /** A glob matching every value key of this namespace, for maintenance scans. */
  String allValueKeysPattern() {
    return namespace + TAG_OPEN + "*" + TAG_CLOSE + VALUE_MARKER + "*";
  }

  /**
   * The metalake of a value key of this namespace, or {@code null} if the key is not one. A scan
   * pattern can match keys of a longer namespace that starts with this one, so the match is checked
   * exactly here.
   */
  String metalakeOfValueKey(String key) {
    String start = namespace + TAG_OPEN;
    if (!key.startsWith(start)) {
      return null;
    }
    int close = key.indexOf(TAG_CLOSE + VALUE_MARKER, start.length());
    if (close <= start.length()) {
      return null;
    }
    String metalake = key.substring(start.length(), close);
    return metalake.contains("{") || metalake.contains("}") ? null : metalake;
  }

  /**
   * The identifier paths whose fences form the stamp of the given identifier: every ancestor from
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
}
