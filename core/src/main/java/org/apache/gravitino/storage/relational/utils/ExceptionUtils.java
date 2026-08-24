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
package org.apache.gravitino.storage.relational.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;

public class ExceptionUtils {
  private ExceptionUtils() {}

  public static void checkSQLException(
      RuntimeException re, Entity.EntityType type, String entityName) throws IOException {
    if (re.getCause() instanceof SQLException) {
      SQLExceptionConverterFactory.getConverter()
          .toGravitinoException((SQLException) re.getCause(), type, entityName);
    }
  }

  /**
   * Creates an {@link OptimisticLockException} for an entity that was modified concurrently by
   * another writer, which makes the version-guarded write fail.
   *
   * @param type The type of the entity that was modified concurrently.
   * @param identifier The identifier of the entity that was modified concurrently.
   * @return The {@link OptimisticLockException} to throw.
   */
  public static OptimisticLockException concurrentModification(
      Entity.EntityType type, NameIdentifier identifier) {
    return new OptimisticLockException(
        "The %s %s was modified concurrently; retry the operation",
        type.name().toLowerCase(Locale.ROOT), identifier);
  }

  /**
   * Creates an {@link OptimisticLockException} for a child entity that was modified concurrently
   * while its parent was being deleted in cascade mode.
   *
   * @param childType The type of the child entity that was modified concurrently.
   * @param parentType The type of the parent entity being operated on.
   * @param parentIdentifier The identifier of the parent entity being operated on.
   * @return The {@link OptimisticLockException} to throw.
   */
  public static OptimisticLockException concurrentChildModification(
      Entity.EntityType childType, Entity.EntityType parentType, NameIdentifier parentIdentifier) {
    return new OptimisticLockException(
        "A %s under %s %s was modified concurrently; retry the operation",
        childType.name().toLowerCase(Locale.ROOT),
        parentType.name().toLowerCase(Locale.ROOT),
        parentIdentifier);
  }
}
