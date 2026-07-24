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
  void testStoresApiAndPreservesProviderKey() {
    KmsReference reference =
        new KmsReference(KmsApi.AWS_KMS, " production ", " alias/Customer-Key ");

    Assertions.assertEquals(KmsApi.AWS_KMS, reference.api());
    Assertions.assertEquals("production", reference.source());
    Assertions.assertEquals(" alias/Customer-Key ", reference.keyId());
  }

  @Test
  void testRejectsMissingFields() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(null, "production", "key"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(KmsApi.AWS_KMS, null, "key"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(KmsApi.AWS_KMS, "production", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(KmsApi.AWS_KMS, "", "key"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(KmsApi.AWS_KMS, " ", "key"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(KmsApi.AWS_KMS, "production", ""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsReference(KmsApi.AWS_KMS, "production", " "));
  }

  @Test
  void testValueSemantics() {
    KmsReference first = new KmsReference(KmsApi.AWS_KMS, "production", "key");
    KmsReference same = new KmsReference(KmsApi.AWS_KMS, "production", "key");
    KmsReference differentApi = new KmsReference(KmsApi.GOOGLE_CLOUD_KMS, "production", "key");
    KmsReference differentSource = new KmsReference(KmsApi.AWS_KMS, "recovery", "key");
    KmsReference differentKey = new KmsReference(KmsApi.AWS_KMS, "production", "another-key");

    Assertions.assertEquals(first, same);
    Assertions.assertEquals(first.hashCode(), same.hashCode());
    Assertions.assertNotEquals(first, differentApi);
    Assertions.assertNotEquals(first, differentSource);
    Assertions.assertNotEquals(first, differentKey);
    Assertions.assertNotEquals(first, null);
    Assertions.assertNotEquals(first, "key");
    Assertions.assertEquals(
        "KmsReference{api=AWS_KMS, source='production', keyId='key'}", first.toString());
  }
}
