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

import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests lossless and backward-compatible entity change-log identifier encoding. */
public class TestEntityChangeLogNameIdentifierCodec {

  @Test
  void testKeepsLegacyEncodingForUnambiguousNames() {
    NameIdentifier ident = NameIdentifier.of("metalake", "catalog", "schema", "table");

    String encoded = EntityChangeLogNameIdentifierCodec.encode(ident);

    Assertions.assertEquals(ident.toString(), encoded);
    Assertions.assertEquals(ident, EntityChangeLogNameIdentifierCodec.decode(encoded));
  }

  @Test
  void testRoundTripsDotsInsideEveryLevel() {
    NameIdentifier ident = NameIdentifier.of("meta.lake", "cat.alog", "sche.ma", "tab.le");

    String encoded = EntityChangeLogNameIdentifierCodec.encode(ident);

    Assertions.assertNotEquals(ident.toString(), encoded);
    Assertions.assertEquals(ident, EntityChangeLogNameIdentifierCodec.decode(encoded));
  }

  @Test
  void testEscapesLegacyValueThatStartsWithCodecPrefix() {
    NameIdentifier ident = NameIdentifier.of("gravitino:v1:metalake");

    String encoded = EntityChangeLogNameIdentifierCodec.encode(ident);

    Assertions.assertEquals(ident, EntityChangeLogNameIdentifierCodec.decode(encoded));
  }

  @Test
  void testRejectsMalformedEncodedValue() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> EntityChangeLogNameIdentifierCodec.decode("gravitino:v1:2:1:a"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> EntityChangeLogNameIdentifierCodec.decode("gravitino:v1:1:x:a"));
  }
}
