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

package org.apache.gravitino.auth;

/**
 * Thrown when the value of the {@link AuthConstants#X_GRAVITINO_ACTIVE_ROLES_HEADER} header is
 * syntactically invalid.
 *
 * <p>This signals a malformed declaration (for example an empty entry, or a reserved keyword
 * combined with another value) as opposed to a well-formed value that names a role the caller does
 * not hold. It extends {@link IllegalArgumentException} so callers can surface it as a {@code 400
 * Bad Request}.
 */
public class IllegalActiveRolesException extends IllegalArgumentException {

  /**
   * Constructs an {@code IllegalActiveRolesException} with the given message.
   *
   * @param message the detail message describing why the header value is invalid
   */
  public IllegalActiveRolesException(String message) {
    super(message);
  }
}
