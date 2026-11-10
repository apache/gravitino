/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.server.web.pagination;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * The opaque page token of the REST API list endpoints.
 *
 * <p>A token is base64url (no padding) of {@code {"v":1,"k":"<last key>","s":"<scope>"}}: the
 * format version, the sort key of the last entry returned, and a hash of the list it was issued
 * for. A token holds no server state, so it never expires and any server node can serve the next
 * page. A token presented to a different list is rejected.
 */
public final class PageToken {

  static final int VERSION = 1;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String FIELD_VERSION = "v";
  private static final String FIELD_KEY = "k";
  private static final String FIELD_SCOPE = "s";
  private static final String MALFORMED = "Invalid pageToken: the token is malformed";
  private static final String WRONG_SCOPE =
      "Invalid pageToken: the token was issued for a different list";

  private PageToken() {}

  /**
   * Computes the scope of a list: the first 16 hex characters of the SHA-256 of its parts.
   *
   * @param parts The parts that identify the list, for example the operation name, the path
   *     parameters and any filter parameters. A null part is treated as distinct from every string.
   * @return The scope string.
   */
  public static String scopeOf(String... parts) {
    StringBuilder sb = new StringBuilder();
    for (String part : parts) {
      // A length prefix keeps ("ab", "c") and ("a", "bc") apart; -1 marks a null part.
      sb.append(part == null ? -1 : part.length()).append(':');
      if (part != null) {
        sb.append(part);
      }
    }
    return Hashing.sha256()
        .hashString(sb.toString(), StandardCharsets.UTF_8)
        .toString()
        .substring(0, 16);
  }

  /**
   * Encodes a token.
   *
   * @param key The sort key of the last entry returned.
   * @param scope The scope of the list, from {@link #scopeOf(String...)}.
   * @return The token.
   */
  public static String encode(String key, String scope) {
    Preconditions.checkArgument(key != null, "key must not be null");
    Preconditions.checkArgument(scope != null, "scope must not be null");
    ObjectNode node = MAPPER.createObjectNode();
    node.put(FIELD_VERSION, VERSION);
    node.put(FIELD_KEY, key);
    node.put(FIELD_SCOPE, scope);
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(node.toString().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Decodes a token and returns the sort key it carries.
   *
   * @param token The token sent by the client.
   * @param expectedScope The scope of the list the request is for.
   * @return The sort key of the last entry of the previous page.
   * @throws IllegalArgumentException If the token is malformed, has an unknown version, or was
   *     issued for a different list. The message names {@code pageToken}.
   */
  public static String decodeKey(String token, String expectedScope) {
    JsonNode node;
    try {
      byte[] json = Base64.getUrlDecoder().decode(token);
      node = MAPPER.readTree(new String(json, StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new IllegalArgumentException(MALFORMED, e);
    }
    if (node == null
        || !node.isObject()
        || !node.path(FIELD_VERSION).isInt()
        || node.path(FIELD_VERSION).intValue() != VERSION
        || !node.path(FIELD_KEY).isTextual()
        || !node.path(FIELD_SCOPE).isTextual()) {
      throw new IllegalArgumentException(MALFORMED);
    }
    if (!node.get(FIELD_SCOPE).textValue().equals(expectedScope)) {
      throw new IllegalArgumentException(WRONG_SCOPE);
    }
    return node.get(FIELD_KEY).textValue();
  }
}
