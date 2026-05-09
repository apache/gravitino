/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service;

import org.apache.iceberg.exceptions.RESTException;

/**
 * Exception for expired authentication tokens, corresponding to the Iceberg REST spec's {@code
 * AuthenticationTimeoutResponse} (HTTP 419).
 *
 * <p>The Iceberg SDK does not ship this exception class, so Gravitino provides it to properly map
 * expired token errors to HTTP 419 per the <a
 * href="https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L292">Iceberg
 * REST catalog OpenAPI spec</a>.
 */
@SuppressWarnings("FormatStringAnnotation")
public class AuthenticationTimeoutException extends RESTException {

  /**
   * Constructs a new exception with the specified detail message.
   *
   * @param message the detail message
   * @param args the arguments to the message
   */
  public AuthenticationTimeoutException(String message, Object... args) {
    super(message, args);
  }

  /**
   * Constructs a new exception with the specified detail message and cause.
   *
   * @param cause the cause
   * @param message the detail message
   * @param args the arguments to the message
   */
  public AuthenticationTimeoutException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }
}
