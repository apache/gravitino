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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.FileTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKeytabNames {

  @Test
  public void testPrefixDependsOnIdentifierUriContentAndModificationTime() {
    byte[] content = "keys".getBytes(StandardCharsets.UTF_8);
    FileTime modified = FileTime.fromMillis(1_000);
    String prefix = KeytabNames.prefix("id", "file:///a.keytab", content, null);
    Assertions.assertEquals(
        prefix, KeytabNames.prefix("id", "file:///a.keytab", content.clone(), null));
    Assertions.assertTrue(prefix.startsWith("id-"), prefix);
    Assertions.assertNotEquals(
        prefix, KeytabNames.prefix("other", "file:///a.keytab", content, null));
    Assertions.assertNotEquals(prefix, KeytabNames.prefix("id", "file:///b.keytab", content, null));
    Assertions.assertNotEquals(
        prefix,
        KeytabNames.prefix(
            "id", "file:///a.keytab", "other".getBytes(StandardCharsets.UTF_8), null));
    // The URI length is hashed too, so moving bytes between the URI and the content changes it.
    Assertions.assertNotEquals(
        KeytabNames.prefix("id", "ab", "c".getBytes(StandardCharsets.UTF_8), null),
        KeytabNames.prefix("id", "a", "bc".getBytes(StandardCharsets.UTF_8), null));
    // Moved across the byte that tells a copy from a link as well.
    Assertions.assertNotEquals(
        KeytabNames.prefix("id", "a\u0000", new byte[] {'c'}, null),
        KeytabNames.prefix("id", "a", new byte[] {0, 'c'}, null));
    // A link is named after the modification time of its target too.
    String linked = KeytabNames.prefix("id", "file:///a.keytab", content, modified);
    Assertions.assertNotEquals(prefix, linked);
    Assertions.assertEquals(
        linked, KeytabNames.prefix("id", "file:///a.keytab", content, FileTime.fromMillis(1_000)));
    Assertions.assertNotEquals(
        linked, KeytabNames.prefix("id", "file:///a.keytab", content, FileTime.fromMillis(1_001)));
  }

  @Test
  public void testReservesTheLowestFreeSlot() {
    String prefix = KeytabNames.prefix("slots", "file:///slots.keytab", new byte[] {1}, null);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    Assertions.assertEquals(1, KeytabNames.reserve(prefix));
    Assertions.assertEquals(2, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 1);
    Assertions.assertEquals(1, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    KeytabNames.release(prefix, 1);
    KeytabNames.release(prefix, 2);
    Assertions.assertFalse(KeytabNames.hasReservedSlots(prefix));
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    // Releasing a slot of a prefix that holds none does nothing.
    KeytabNames.release(prefix, 5);
    Assertions.assertEquals(0, KeytabNames.reserve(prefix));
    KeytabNames.release(prefix, 0);
    Assertions.assertFalse(KeytabNames.hasReservedSlots(prefix));
  }

  @Test
  public void testDirectoryIsUnderTheWorkingDirectory() {
    Assertions.assertEquals(new File("keytabs"), KeytabNames.directory());
  }
}
