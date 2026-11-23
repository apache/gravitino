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
package org.apache.gravitino.utils;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKeytabNames {

  @Test
  public void testPrefixDependsOnIdentifierUriAndContent() {
    byte[] content = "keys".getBytes(StandardCharsets.UTF_8);
    String prefix = KeytabNames.prefix("id", "file:///a.keytab", content);
    Assertions.assertEquals(prefix, KeytabNames.prefix("id", "file:///a.keytab", content.clone()));
    Assertions.assertTrue(prefix.startsWith("id-" + KeytabNames.token() + "-"), prefix);
    Assertions.assertNotEquals(prefix, KeytabNames.prefix("other", "file:///a.keytab", content));
    Assertions.assertNotEquals(prefix, KeytabNames.prefix("id", "file:///b.keytab", content));
    Assertions.assertNotEquals(
        prefix,
        KeytabNames.prefix("id", "file:///a.keytab", "other".getBytes(StandardCharsets.UTF_8)));
    // The URI length is hashed too, so moving bytes between the URI and the content changes it.
    Assertions.assertNotEquals(
        KeytabNames.prefix("id", "ab", "c".getBytes(StandardCharsets.UTF_8)),
        KeytabNames.prefix("id", "a", "bc".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testReservesTheLowestFreeSlot() {
    String prefix = KeytabNames.prefix("slots", "file:///slots.keytab", new byte[] {1});
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    Assertions.assertEquals(1, KeytabNames.reserve(prefix));
    Assertions.assertEquals(2, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 1);
    Assertions.assertEquals(1, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    KeytabNames.release(prefix, 1);
    KeytabNames.release(prefix, 2);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    // Releasing a slot of a prefix that holds none does nothing.
    KeytabNames.release(prefix, 5);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
  }
}
