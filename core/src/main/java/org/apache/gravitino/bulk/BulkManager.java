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
package org.apache.gravitino.bulk;

import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.dto.responses.BulkError;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;

/** Manages best-effort bulk operations. */
public class BulkManager {

  /** The request field checked by a bulk operation. */
  public enum BulkRequestField {
    /** The users field in a bulk user add request. */
    USERS("users"),

    /** The names field in a bulk remove request. */
    NAMES("names");

    private final String fieldName;

    BulkRequestField(String fieldName) {
      this.fieldName = fieldName;
    }
  }

  private final int maxBulkItems;

  /**
   * Creates a new {@link BulkManager}.
   *
   * @param config The Gravitino configuration.
   */
  public BulkManager(Config config) {
    this.maxBulkItems =
        config == null
            ? Configs.BULK_MAX_ITEMS.getDefaultValue()
            : config.get(Configs.BULK_MAX_ITEMS);
  }

  /**
   * Checks whether the request item size exceeds the configured bulk limit.
   *
   * @param field The request field.
   * @param size The request item size.
   */
  public void checkBulkSize(BulkRequestField field, int size) {
    if (size > maxBulkItems) {
      throw new IllegalArgumentException(
          String.format(
              "\"%s\" size %d exceeds the maximum allowed bulk items %d",
              field.fieldName, size, maxBulkItems));
    }
  }

  private int errorCode(Exception e) {
    if (e instanceof IllegalArgumentException) {
      return ErrorConstants.ILLEGAL_ARGUMENTS_CODE;
    } else if (e instanceof NotFoundException) {
      return ErrorConstants.NOT_FOUND_CODE;
    } else if (e instanceof AlreadyExistsException) {
      return ErrorConstants.ALREADY_EXISTS_CODE;
    } else if (e instanceof ForbiddenException) {
      return ErrorConstants.FORBIDDEN_CODE;
    } else if (e instanceof NotInUseException) {
      return ErrorConstants.NOT_IN_USE_CODE;
    }
    return ErrorConstants.INTERNAL_ERROR_CODE;
  }

  /**
   * Converts an item-level exception to a bulk error.
   *
   * @param result The failed item result.
   * @return The bulk error.
   */
  public BulkError toBulkError(BulkItemResult<?> result) {
    Exception error =
        result
            .error()
            .orElseThrow(() -> new IllegalArgumentException("Bulk item result has no error"));
    return new BulkError(
        result.index(),
        result.name(),
        errorCode(error),
        error.getClass().getSimpleName(),
        error.getMessage());
  }
}
