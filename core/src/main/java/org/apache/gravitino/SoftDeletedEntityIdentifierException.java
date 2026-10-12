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

/**
 * Indicates that an entity cannot be created because its stable identifier is retained by a
 * soft-deleted record.
 */
public class SoftDeletedEntityIdentifierException extends EntityAlreadyExistsException {

  /**
   * Constructs a {@code SoftDeletedEntityIdentifierException}.
   *
   * @param entityType the entity type
   * @param entityId the stable entity identifier
   */
  public SoftDeletedEntityIdentifierException(Entity.EntityType entityType, long entityId) {
    super(
        "The %s identifier %s is retained by a soft-deleted record",
        entityType.name().toLowerCase(), entityId);
  }

  /**
   * Constructs a {@code SoftDeletedEntityIdentifierException} with its underlying conflict.
   *
   * @param cause the underlying duplicate-identifier failure
   * @param entityType the entity type
   * @param entityId the stable entity identifier
   */
  public SoftDeletedEntityIdentifierException(
      Throwable cause, Entity.EntityType entityType, long entityId) {
    super(
        cause,
        "The %s identifier %s is retained by a soft-deleted record",
        entityType.name().toLowerCase(),
        entityId);
  }
}
