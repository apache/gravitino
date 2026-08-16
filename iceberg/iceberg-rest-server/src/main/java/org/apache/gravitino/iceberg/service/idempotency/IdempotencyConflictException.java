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
package org.apache.gravitino.iceberg.service.idempotency;

/**
 * Thrown when an {@code Idempotency-Key} cannot be honored: the first request using it is still
 * running, or the key was already used for a different operation. Both cases are reported to the
 * client as {@code 409 Conflict}.
 */
public class IdempotencyConflictException extends RuntimeException {

  /**
   * Creates a conflict exception.
   *
   * @param message the message returned to the client
   */
  public IdempotencyConflictException(String message) {
    super(message);
  }
}
