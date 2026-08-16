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
package org.apache.gravitino.iceberg.common.idempotency;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIdempotencyKeys {

  @ParameterizedTest
  @ValueSource(
      strings = {
        // The example from the Iceberg REST spec, in both cases.
        "017F22E2-79B0-7CC3-98C4-DC0C0C07398F",
        "017f22e2-79b0-7cc3-98c4-dc0c0c07398f",
        // Variant octet 0b10xx, covering the whole RFC 9562 variant range.
        "017f22e2-79b0-7cc3-88c4-dc0c0c07398f",
        "017f22e2-79b0-7cc3-b8c4-dc0c0c07398f"
      })
  void testAcceptsUuidV7(String idempotencyKey) {
    Assertions.assertTrue(IdempotencyKeys.isValid(idempotencyKey));
    Assertions.assertDoesNotThrow(() -> IdempotencyKeys.validate(idempotencyKey));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        // UUIDv4, the version nibble must be 7.
        "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        // UUIDv1.
        "c232ab00-9414-11ec-b3c8-9f6bdeced846",
        // RFC 9562 requires the variant bits to be 10; this is the NCS variant.
        "017f22e2-79b0-7cc3-28c4-dc0c0c07398f",
        // The Microsoft variant.
        "017f22e2-79b0-7cc3-c8c4-dc0c0c07398f",
        // Not the canonical 36-character form.
        "017f22e279b07cc398c4dc0c0c07398f",
        "017f22e2-79b0-7cc3-98c4-dc0c0c07398",
        "017f22e2-79b0-7cc3-98c4-dc0c0c07398ff",
        "  017f22e2-79b0-7cc3-98c4-dc0c0c07398f  ",
        // Right length, but not hexadecimal.
        "017f22e2-79b0-7cc3-98c4-dc0c0c0739zz",
        "not-a-uuid"
      })
  void testRejectsAnythingElse(String idempotencyKey) {
    Assertions.assertFalse(IdempotencyKeys.isValid(idempotencyKey));
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> IdempotencyKeys.validate(idempotencyKey));
    Assertions.assertTrue(exception.getMessage().contains("UUIDv7"), exception.getMessage());
  }

  @Test
  void testNilUuidIsRejected() {
    Assertions.assertFalse(IdempotencyKeys.isValid("00000000-0000-0000-0000-000000000000"));
  }
}
