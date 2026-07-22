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

import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Common inspection contract for implemented KMS clients. */
public abstract class TestKmsClientContract {

  /**
   * Returns the client under test.
   *
   * @return the client
   */
  protected abstract KmsClient client();

  /**
   * Returns a reference to an enabled key that supports wrapping and unwrapping.
   *
   * @return the usable key reference
   */
  protected abstract KmsReference usableKey();

  /**
   * Returns a reference to a key that does not exist.
   *
   * @return the missing key reference
   */
  protected abstract KmsReference missingKey();

  @Test
  void testReportsUsableKeyProperties() {
    KmsReference reference = usableKey();
    KmsKeyProperties properties = client().getKeyProperties(reference);

    Assertions.assertEquals(reference, properties.reference());
    Assertions.assertTrue(properties.present());
    Assertions.assertTrue(properties.enabled());
    Assertions.assertTrue(properties.supportsWrapping());
    Assertions.assertTrue(properties.supportsUnwrapping());
  }

  @Test
  void testReportsMissingKey() {
    KmsReference reference = missingKey();
    KmsKeyProperties properties = client().getKeyProperties(reference);

    Assertions.assertEquals(reference, properties.reference());
    Assertions.assertFalse(properties.present());
  }

  @Test
  void testRejectsNullReference() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> client().getKeyProperties(null));
  }

  @Test
  void testRejectsMismatchedSource() {
    KmsReference reference = usableKey();
    KmsReference mismatched =
        new KmsReference(reference.api(), reference.source() + "-other", reference.keyId());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> client().getKeyProperties(mismatched));
  }

  @Test
  void testRejectsMismatchedApi() {
    KmsReference reference = usableKey();
    KmsApi otherApi =
        Arrays.stream(KmsApi.values())
            .filter(api -> api != reference.api())
            .findFirst()
            .orElseThrow(IllegalStateException::new);
    KmsReference mismatched = new KmsReference(otherApi, reference.source(), reference.keyId());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> client().getKeyProperties(mismatched));
  }
}
