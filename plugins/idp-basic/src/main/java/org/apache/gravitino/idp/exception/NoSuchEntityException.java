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
package org.apache.gravitino.idp.exception;

/** This exception is thrown when a built-in IdP entity is not found. */
public class NoSuchEntityException extends RuntimeException {
  /** The no such entity message for the exception. */
  public static final String NO_SUCH_ENTITY_MESSAGE = "No such %s entity: %s";

  /**
   * Constructs a new NoSuchEntityException for the given entity type and name.
   *
   * @param entityType human-readable entity type, for example {@code idp user}
   * @param entityName entity identifier, for example username or group name
   */
  public NoSuchEntityException(String entityType, String entityName) {
    super(String.format(NO_SUCH_ENTITY_MESSAGE, entityType, entityName));
  }

  /**
   * Constructs a new NoSuchEntityException for the given entity type and name.
   *
   * @param cause the cause of the exception
   * @param entityType human-readable entity type, for example {@code idp user}
   * @param entityName entity identifier, for example username or group name
   */
  public NoSuchEntityException(Throwable cause, String entityType, String entityName) {
    super(String.format(NO_SUCH_ENTITY_MESSAGE, entityType, entityName), cause);
  }
}
