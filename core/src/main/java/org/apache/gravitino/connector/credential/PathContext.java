/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.connector.credential;

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * The {@code PathContext} class represents the path and its associated credential type to generate
 * a credential for {@link org.apache.gravitino.credential.CredentialOperationDispatcher}.
 */
@DeveloperApi
public class PathContext {

  private final String path;

  private final String credentialType;

  /**
   * Constructs a new {@code PathContext} instance with the given path and credential type.
   *
   * @param path The path string.
   * @param credentialType The type of the credential.
   */
  public PathContext(String path, String credentialType) {
    this.path = path;
    this.credentialType = credentialType;
  }

  /**
   * Gets the path string.
   *
   * @return The path associated with this instance.
   */
  public String path() {
    return path;
  }

  /**
   * Gets the credential type.
   *
   * @return The credential type associated with this instance.
   */
  public String credentialType() {
    return credentialType;
  }
}
