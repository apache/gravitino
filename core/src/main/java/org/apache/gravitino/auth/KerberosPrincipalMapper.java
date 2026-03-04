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

import java.security.Principal;

/**
 * Kerberos principal mapper that parses Kerberos principal strings into {@link KerberosPrincipal}
 * objects.
 *
 * <p>This mapper is thread-safe and can be shared across multiple authentication requests.
 */
public class KerberosPrincipalMapper implements PrincipalMapper {

  /**
   * Maps a Kerberos principal string to a KerberosPrincipal object.
   *
   * @param principal the Kerberos principal string to parse (e.g., "user/instance@REALM")
   * @return a KerberosPrincipal object containing the parsed components
   * @throws IllegalArgumentException if the principal string is null, empty, or has invalid format
   */
  @Override
  public Principal map(String principal) {
    if (principal == null || principal.isEmpty()) {
      throw new IllegalArgumentException("Principal string cannot be null or empty");
    }

    // Kerberos principal format: user[/instance][@REALM]
    // Find positions of '/' and '@'
    int slashIndex = principal.indexOf('/');
    int atIndex = principal.indexOf('@');

    String username;
    String instance = null;
    String realm = null;

    // Extract username (up to '/' or '@', whichever comes first)
    if (slashIndex >= 0 && atIndex >= 0) {
      // Both '/' and '@' present
      if (slashIndex < atIndex) {
        // Format: user/instance@REALM
        username = principal.substring(0, slashIndex);
        instance = principal.substring(slashIndex + 1, atIndex);
        realm = principal.substring(atIndex + 1);
      } else {
        // Invalid format: '@' before '/'
        throw new IllegalArgumentException("Invalid Kerberos principal format: " + principal);
      }
    } else if (slashIndex >= 0) {
      // Only '/' present: user/instance
      username = principal.substring(0, slashIndex);
      instance = principal.substring(slashIndex + 1);
    } else if (atIndex >= 0) {
      // Only '@' present: user@REALM
      username = principal.substring(0, atIndex);
      realm = principal.substring(atIndex + 1);
    } else {
      // No delimiters: just username
      username = principal;
    }

    // Validate username is not empty
    if (username.isEmpty()) {
      throw new IllegalArgumentException("Username cannot be empty in principal: " + principal);
    }

    return new KerberosPrincipal(username, instance, realm);
  }
}
