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
package org.apache.gravitino.iceberg.common.rest.auth;

import java.security.Principal;
import java.util.Map;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

/**
 * Iceberg REST catalog {@link AuthManager} that adds an {@code Authorization} header on each
 * outgoing request using the access token from the current {@link UserPrincipal}.
 *
 * <p>Enable by setting {@link org.apache.iceberg.rest.auth.AuthProperties#AUTH_TYPE} to this class
 * name, or set catalog property {@code gravitino.iceberg-rest-catalog.forward-user-access-token} to
 * {@code true} for REST catalog backend (see {@link
 * org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil#mergeRestCatalogAuthForUserPrincipal}).
 */
public class UserPrincipalForwardingAuthManager implements AuthManager {

  /**
   * @param name catalog name passed by Iceberg when loading this auth manager
   */
  @SuppressWarnings("unused")
  public UserPrincipalForwardingAuthManager(String name) {}

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    return new UserPrincipalAuthSession();
  }

  @Override
  public void close() {
    // no resources
  }

  private static final class UserPrincipalAuthSession implements AuthSession {
    @Override
    public HTTPRequest authenticate(HTTPRequest request) {
      Principal principal = PrincipalUtils.getCurrentPrincipal();
      if (!(principal instanceof UserPrincipal)) {
        throw new IllegalStateException(
            "Current principal must be a UserPrincipal to forward access token to Iceberg REST");
      }
      String authorizationHeaderValue =
          ((UserPrincipal) principal)
              .getAccessToken()
              .orElseThrow(
                  () ->
                      new IllegalStateException("UserPrincipal has no authorization header value"));
      HTTPHeaders newHeaders =
          request
              .headers()
              .putIfAbsent(HTTPHeaders.of(Map.of("Authorization", authorizationHeaderValue)));
      return newHeaders.equals(request.headers())
          ? request
          : ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
    }

    @Override
    public void close() {}
  }
}
