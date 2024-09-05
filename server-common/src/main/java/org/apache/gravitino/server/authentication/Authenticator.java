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

package org.apache.gravitino.server.authentication;

import java.security.Principal;
import org.apache.gravitino.Config;

/** The interface provides authentication mechanism. */
public interface Authenticator {

  /**
   * Judge whether the data used to authenticate is from the token.
   *
   * @return true, if the data used to authenticate is from the token, Otherwise, it's false.
   */
  default boolean isDataFromToken() {
    return false;
  }

  /**
   * Use the token data to authenticate.
   *
   * @param tokenData The data is used for authentication
   * @return The identifier of user
   */
  default Principal authenticateToken(byte[] tokenData) {
    throw new UnsupportedOperationException(
        "Authenticator doesn't support to authenticate the data from the token");
  }

  /**
   * Initialize the authenticator
   *
   * <p>Note. This method will be called after the Authenticator object is created, and before any *
   * other methods are called.
   *
   * @param config The config for authenticator
   * @throws RuntimeException if the initialization fails
   */
  void initialize(Config config) throws RuntimeException;

  /**
   * Determines if the provided token data is supported by this authenticator
   *
   * <p>This method checks if the given token data can be processed by this authenticator.
   * Implementations should override this method to provide specific logic for determining if the
   * token data format or content is recognized and can be authenticated.
   *
   * @param tokenData The byte array containing the token data to be checked.
   * @return true if the token data is supported and can be authenticated by this authenticator;
   *     false otherwise.
   */
  default boolean supportsToken(byte[] tokenData) {
    return false;
  }
}
