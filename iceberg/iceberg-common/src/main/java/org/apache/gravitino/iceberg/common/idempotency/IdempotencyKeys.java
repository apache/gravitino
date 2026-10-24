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

import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Validation helpers for the {@code Idempotency-Key} header.
 *
 * <p>The Iceberg REST spec requires the header value to be a UUIDv7 in string form as defined by <a
 * href="https://www.rfc-editor.org/rfc/rfc9562">RFC 9562</a>, with a fixed length of 36 characters.
 * Rejecting malformed keys before touching the store keeps invalid requests off the storage path
 * and guarantees the key space stays time-ordered, which keeps the primary-key index of a database
 * backed store insert-friendly.
 */
public final class IdempotencyKeys {

  /** Canonical (hyphenated) length of a UUID in string form, mandated by the Iceberg REST spec. */
  public static final int KEY_LENGTH = 36;

  /** UUID version reported by a UUIDv7 as defined by RFC 9562. */
  private static final int UUID_VERSION_7 = 7;

  /**
   * Variant reported by {@link UUID#variant()} for the RFC 9562 variant, whose two most significant
   * variant bits are {@code 10}.
   */
  private static final int RFC_9562_VARIANT = 2;

  private static final Pattern CANONICAL_UUID =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

  private IdempotencyKeys() {}

  /**
   * Returns whether the value is a UUIDv7 in canonical string form.
   *
   * @param idempotencyKey the raw header value, may be {@code null}
   * @return {@code true} if the value is a valid RFC 9562 UUIDv7
   */
  public static boolean isValid(String idempotencyKey) {
    if (idempotencyKey == null || idempotencyKey.length() != KEY_LENGTH) {
      return false;
    }
    if (!CANONICAL_UUID.matcher(idempotencyKey).matches()) {
      return false;
    }
    UUID uuid = UUID.fromString(idempotencyKey);
    return uuid.version() == UUID_VERSION_7 && uuid.variant() == RFC_9562_VARIANT;
  }

  /**
   * Validates the value and throws when it is not a UUIDv7 in canonical string form.
   *
   * @param idempotencyKey the raw header value, may be {@code null}
   * @throws IllegalArgumentException if the value is not a valid RFC 9562 UUIDv7, which the Iceberg
   *     REST server maps to {@code 400 Bad Request}
   */
  public static void validate(String idempotencyKey) {
    if (!isValid(idempotencyKey)) {
      throw new IllegalArgumentException(
          "Idempotency-Key: "
              + idempotencyKey
              + " is illegal, the Iceberg REST spec requires a UUIDv7 in string form (RFC 9562), "
              + "for example 017f22e2-79b0-7cc3-98c4-dc0c0c07398f");
    }
  }

  /**
   * Validates the value and returns it in the lower-case form used for storage and comparison.
   *
   * <p>RFC 9562 section 4 defines the hexadecimal digits of a UUID as case-insensitive on input
   * while specifying lower case on output, so {@code 017F22E2-...} and {@code 017f22e2-...} name
   * the same key. A client that retries with a differently-cased key is retrying the same logical
   * operation, and folding the case here is what keeps that retry from being taken for a new
   * request and re-running the mutation. It also keeps a database-backed store correct under a
   * case-sensitive collation such as {@code utf8mb4_bin}, since only the folded form is ever
   * written.
   *
   * @param idempotencyKey the raw header value, may be {@code null}
   * @return the key folded to lower case
   * @throws IllegalArgumentException if the value is not a valid RFC 9562 UUIDv7
   */
  public static String canonicalize(String idempotencyKey) {
    validate(idempotencyKey);
    return idempotencyKey.toLowerCase(Locale.ROOT);
  }
}
