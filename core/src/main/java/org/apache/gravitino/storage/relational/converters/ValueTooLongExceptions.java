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
package org.apache.gravitino.storage.relational.converters;

import java.sql.SQLException;
import java.util.Locale;
import org.apache.gravitino.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds the exception for a SQL error reporting a value that is too long for its column. */
final class ValueTooLongExceptions {
  private static final Logger LOG = LoggerFactory.getLogger(ValueTooLongExceptions.class);

  private ValueTooLongExceptions() {}

  /**
   * Creates the exception for a SQL error reporting a value that is too long for its column.
   *
   * <p>The SQL exception is logged but not attached as the cause, since the error response
   * serializes the whole stack trace and would expose the database error message to the client.
   *
   * @param sqlException The SQL exception reporting the value that is too long.
   * @param type The type of the entity being persisted.
   * @param name The name of the entity being persisted.
   * @return The {@link IllegalArgumentException} to throw.
   */
  static IllegalArgumentException of(
      SQLException sqlException, Entity.EntityType type, String name) {
    LOG.warn("Failed to persist the {} entity: {}", type, name, sqlException);
    return new IllegalArgumentException(
        String.format(
            "The %s entity has a value that exceeds the maximum length of its column.",
            type.name().toLowerCase(Locale.ROOT)));
  }
}
