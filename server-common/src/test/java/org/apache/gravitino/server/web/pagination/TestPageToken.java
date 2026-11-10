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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPageToken {

  private static final String SCOPE = PageToken.scopeOf("listTables", "lake", "cat", "sch");

  @Test
  public void testRoundTrip() {
    String token = PageToken.encode("orders_2025", SCOPE);
    Assertions.assertEquals("orders_2025", PageToken.decodeKey(token, SCOPE));
  }

  @Test
  public void testRoundTripNonAsciiKey() {
    String key = "表_ü_\"quoted\"";
    Assertions.assertEquals(key, PageToken.decodeKey(PageToken.encode(key, SCOPE), SCOPE));
  }

  @Test
  public void testTokenIsUrlSafe() {
    String token = PageToken.encode("a/b+c?d", SCOPE);
    Assertions.assertTrue(token.matches("[A-Za-z0-9_-]+"), token);
  }

  @Test
  public void testScopeSeparatesParts() {
    Assertions.assertNotEquals(PageToken.scopeOf("ab", "c"), PageToken.scopeOf("a", "bc"));
    Assertions.assertNotEquals(PageToken.scopeOf("a", null), PageToken.scopeOf("a", "null"));
    Assertions.assertEquals(PageToken.scopeOf("a", "b"), PageToken.scopeOf("a", "b"));
    Assertions.assertEquals(16, SCOPE.length());
  }

  @Test
  public void testRejectsTokenFromAnotherList() {
    String token = PageToken.encode("t1", SCOPE);
    String other = PageToken.scopeOf("listTables", "lake", "cat", "other");
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> PageToken.decodeKey(token, other));
    Assertions.assertTrue(e.getMessage().contains("pageToken"), e.getMessage());
  }

  @Test
  public void testRejectsMalformedTokens() {
    String[] bad = {
      "",
      "!!!not base64!!!",
      encodeRaw("not json"),
      encodeRaw("[1,2,3]"),
      encodeRaw("{\"v\":2,\"k\":\"t1\",\"s\":\"" + SCOPE + "\"}"),
      encodeRaw("{\"v\":\"1\",\"k\":\"t1\",\"s\":\"" + SCOPE + "\"}"),
      encodeRaw("{\"v\":1,\"s\":\"" + SCOPE + "\"}"),
      encodeRaw("{\"v\":1,\"k\":7,\"s\":\"" + SCOPE + "\"}"),
      encodeRaw("{\"v\":1,\"k\":\"t1\"}")
    };
    for (String token : bad) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> PageToken.decodeKey(token, SCOPE),
              "token should be rejected: " + token);
      Assertions.assertTrue(e.getMessage().contains("pageToken"), e.getMessage());
    }
  }

  private static String encodeRaw(String json) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(json.getBytes(StandardCharsets.UTF_8));
  }
}
