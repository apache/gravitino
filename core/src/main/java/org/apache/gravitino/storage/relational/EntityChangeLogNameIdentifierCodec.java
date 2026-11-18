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
package org.apache.gravitino.storage.relational;

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;

/** Losslessly encodes entity identifiers in the change log while retaining legacy readability. */
public final class EntityChangeLogNameIdentifierCodec {

  private static final String ENCODED_PREFIX = "gravitino:v1:";

  private EntityChangeLogNameIdentifierCodec() {}

  /**
   * Encodes an identifier for the {@code entity_full_name} change-log column.
   *
   * <p>Identifiers without ambiguous dots retain their historical dot-joined representation, so
   * older nodes can continue consuming ordinary names during a rolling upgrade. Identifiers whose
   * segments contain dots use a length-prefixed representation.
   *
   * @param ident identifier to encode
   * @return encoded identifier
   */
  public static String encode(NameIdentifier ident) {
    String legacyValue = ident.toString();
    String[] levels = levels(ident);
    boolean requiresEncoding =
        legacyValue.startsWith(ENCODED_PREFIX)
            || Arrays.stream(levels).anyMatch(level -> level.indexOf('.') >= 0);
    if (!requiresEncoding) {
      return legacyValue;
    }

    StringBuilder encoded = new StringBuilder(ENCODED_PREFIX).append(levels.length).append(':');
    for (String level : levels) {
      encoded.append(level.length()).append(':').append(level);
    }
    return encoded.toString();
  }

  /**
   * Decodes an identifier read from the {@code entity_full_name} change-log column.
   *
   * @param encoded encoded identifier
   * @return decoded identifier
   * @throws IllegalArgumentException if the encoded value is malformed
   */
  public static NameIdentifier decode(String encoded) {
    if (encoded == null || encoded.isEmpty()) {
      throw new IllegalArgumentException("Cannot decode a null or empty entity identifier");
    }
    if (!encoded.startsWith(ENCODED_PREFIX)) {
      return NameIdentifier.parse(encoded);
    }

    int offset = ENCODED_PREFIX.length();
    int separator = encoded.indexOf(':', offset);
    int levelCount = parseLength(encoded, offset, separator);
    if (levelCount <= 0) {
      throw new IllegalArgumentException("Entity identifier must contain at least one level");
    }

    String[] levels = new String[levelCount];
    offset = separator + 1;
    for (int index = 0; index < levelCount; index++) {
      separator = encoded.indexOf(':', offset);
      int levelLength = parseLength(encoded, offset, separator);
      offset = separator + 1;
      if (levelLength <= 0 || levelLength > encoded.length() - offset) {
        throw new IllegalArgumentException("Invalid entity identifier level length");
      }
      levels[index] = encoded.substring(offset, offset + levelLength);
      offset += levelLength;
    }
    if (offset != encoded.length()) {
      throw new IllegalArgumentException("Unexpected trailing data in entity identifier");
    }
    return NameIdentifier.of(levels);
  }

  private static String[] levels(NameIdentifier ident) {
    String[] namespace = ident.namespace().levels();
    String[] levels = Arrays.copyOf(namespace, namespace.length + 1);
    levels[namespace.length] = ident.name();
    return levels;
  }

  private static int parseLength(String encoded, int offset, int separator) {
    if (separator <= offset) {
      throw new IllegalArgumentException("Missing entity identifier length");
    }
    try {
      return Integer.parseInt(encoded.substring(offset, separator));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid entity identifier length", e);
    }
  }
}
