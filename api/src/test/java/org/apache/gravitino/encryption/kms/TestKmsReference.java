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
package org.apache.gravitino.encryption.kms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsReference {

  @Test
  void testTrimsProviderAndPreservesKeyId() {
    KmsReference reference = new KmsReference(" production ", " alias/Customer-Key ");

    Assertions.assertEquals("production", reference.provider());
    Assertions.assertEquals(" alias/Customer-Key ", reference.keyId());
  }

  @Test
  void testRejectsMissingFields() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> new KmsReference(null, "key"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new KmsReference("", "key"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new KmsReference(" ", "key"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference("production", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference("production", ""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference("production", " "));
  }

  @Test
  void testValueSemantics() {
    KmsReference first = new KmsReference("production", "key");
    KmsReference same = new KmsReference("production", "key");
    KmsReference differentProvider = new KmsReference("recovery", "key");
    KmsReference differentKey = new KmsReference("production", "another-key");

    Assertions.assertEquals(first, same);
    Assertions.assertEquals(first.hashCode(), same.hashCode());
    Assertions.assertNotEquals(first, differentProvider);
    Assertions.assertNotEquals(first, differentKey);
    Assertions.assertNotEquals(first, null);
    Assertions.assertNotEquals(first, "key");
    Assertions.assertEquals("KmsReference{provider='production', keyId='key'}", first.toString());
  }
}
