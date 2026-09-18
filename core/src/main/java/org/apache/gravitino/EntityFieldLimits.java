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
package org.apache.gravitino;

import java.util.Locale;
import javax.annotation.Nullable;

/**
 * The maximum lengths of entity fields persisted by the relational entity store.
 *
 * <p>These values must match the column definitions in {@code
 * scripts/{h2,mysql,postgresql}/schema-*.sql}, so that values which cannot be stored are rejected
 * before they reach the database.
 */
public final class EntityFieldLimits {

  /** The maximum number of characters of an entity name or a model version alias. */
  public static final int MAX_NAME_LENGTH = 128;

  /** The maximum number of characters of an entity comment stored in a 256-character column. */
  public static final int MAX_COMMENT_LENGTH = 256;

  private EntityFieldLimits() {}

  /**
   * Checks that a value does not exceed the given maximum number of characters, counted in Unicode
   * code points.
   *
   * @param value The value to check, a null value always passes.
   * @param maxLength The maximum number of characters allowed.
   * @param fieldName The name of the field, used in the error message.
   * @param entityType The type of the entity owning the field, used in the error message.
   * @throws IllegalArgumentException If the value exceeds the maximum length.
   */
  public static void checkMaxLength(
      @Nullable String value,
      int maxLength,
      String fieldName,
      @Nullable Entity.EntityType entityType) {
    // Count code points rather than UTF-16 chars, since MySQL (utf8mb4) and PostgreSQL count a
    // supplementary character such as an emoji as one character.
    if (value != null
        && value.length() > maxLength
        && value.codePointCount(0, value.length()) > maxLength) {
      throw new IllegalArgumentException(exceedMaxLengthMessage(fieldName, entityType, maxLength));
    }
  }

  private static String exceedMaxLengthMessage(
      String fieldName, @Nullable Entity.EntityType entityType, int maxLength) {
    if (entityType == null) {
      return String.format("Field %s must not exceed %d characters", fieldName, maxLength);
    }

    return String.format(
        "The %s of the %s must not exceed %d characters",
        fieldName, entityType.name().toLowerCase(Locale.ROOT).replace('_', ' '), maxLength);
  }
}
