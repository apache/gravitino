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

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * Shared name-path rules for entity-cache hierarchy scans.
 *
 * <p>These prefixes are the same ones {@link CaffeineEntityCache} uses when cascading invalidation:
 * a bare name-level boundary so {@code catalog1} never matches {@code catalog10}, plus the
 * hierarchical-schema separator when the entity is a schema.
 */
public final class EntityCacheHierarchy {

  /**
   * Separates {@link NameIdentifier} levels in a cache index key. Matching on this boundary (not
   * the bare identifier) keeps sibling prefix scans exact.
   */
  public static final String NAME_LEVEL_BOUNDARY = ".";

  /** Escapes {@link #NAME_LEVEL_BOUNDARY} and itself inside a level of an index key. */
  static final char INDEX_ESCAPE = '\u0001';

  private EntityCacheHierarchy() {}

  /**
   * Joins the levels of an identifier into prefix-index text.
   *
   * <p>{@link NameIdentifier#toString()} is not one-to-one when a level contains {@code "."}.
   * Escaping {@code "."} and the escape character inside each level makes the text unique, while
   * every real level boundary stays a bare {@code "."}.
   *
   * @param ident The identifier to encode.
   * @return The escaped index text for {@code ident}.
   */
  public static String indexText(NameIdentifier ident) {
    StringBuilder text = new StringBuilder();
    for (String level : ident.namespace().levels()) {
      appendEscaped(text, level).append(NAME_LEVEL_BOUNDARY);
    }
    return appendEscaped(text, ident.name()).toString();
  }

  /**
   * Returns the descendant index prefixes used when cascading from {@code ident}.
   *
   * <p>Does not include {@code ident} itself. A concurrent reader whose {@link #indexText} starts
   * with one of these prefixes is under the same subtree that hierarchy invalidation would clear.
   *
   * @param ident The ancestor identifier.
   * @param type The ancestor entity type.
   * @return One or two prefixes ending at a name-level or schema-separator boundary.
   */
  public static List<String> descendantPrefixes(NameIdentifier ident, Entity.EntityType type) {
    String identifier = indexText(ident);
    List<String> prefixes = new ArrayList<>(2);
    prefixes.add(identifier + NAME_LEVEL_BOUNDARY);
    if (type == Entity.EntityType.SCHEMA) {
      prefixes.add(identifier + HierarchicalSchemaUtil.schemaSeparator());
    }
    return prefixes;
  }

  /**
   * Returns whether {@code ident} lies under {@code prefix} in index space.
   *
   * @param ident The identifier to test.
   * @param prefix A prefix from {@link #descendantPrefixes}.
   * @return {@code true} if {@code ident} is a descendant matched by {@code prefix}.
   */
  public static boolean isUnderPrefix(NameIdentifier ident, String prefix) {
    return indexText(ident).startsWith(prefix);
  }

  /**
   * Appends a level to prefix-index text, prefixing {@code "."} and the escape character with the
   * escape character.
   */
  static StringBuilder appendEscaped(StringBuilder text, String level) {
    for (int i = 0; i < level.length(); i++) {
      char c = level.charAt(i);
      if (c == INDEX_ESCAPE || c == NAME_LEVEL_BOUNDARY.charAt(0)) {
        text.append(INDEX_ESCAPE);
      }
      text.append(c);
    }
    return text;
  }
}
