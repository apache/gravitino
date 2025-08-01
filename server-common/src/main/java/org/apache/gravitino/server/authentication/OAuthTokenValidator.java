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

/**
 * Interface for OAuth token validators that can be plugged into the OAuth2TokenAuthenticator.
 * Implementations should handle provider-specific token validation logic.
 */
public interface OAuthTokenValidator {

  /**
   * Initialize the validator with the provided configuration.
   *
   * @param config The configuration object containing OAuth settings
   */
  void initialize(Config config);

  /**
   * Validate the given JWT token and return a Principal if valid.
   *
   * @param token The JWT token to validate
   * @param serviceAudience The expected audience for the token
   * @return A Principal representing the authenticated user
   * @throws org.apache.gravitino.exceptions.UnauthorizedException if token validation fails
   */
  Principal validateToken(String token, String serviceAudience);
}
