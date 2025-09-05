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

package org.apache.gravitino.cli;

/** Represents the OAuth data required to authenticate with an OAuth server. */
public class OAuthData {
  /** The URI of the OAuth server. */
  protected final String serverURI;
  /** The credential used for authentication. */
  protected final String credential;
  /** The access token obtained after authentication. */
  protected final String token;
  /** The scope of access granted by the OAuth token. */
  protected final String scope;

  /**
   * Constructs an {@code OAuthData} instance with the specified server URI, credential, token, and
   * scope.
   *
   * @param serverURI the URI of the OAuth server
   * @param credential the credential used for authentication
   * @param token the access token obtained after authentication
   * @param scope the scope of access granted by the OAuth token
   */
  public OAuthData(String serverURI, String credential, String token, String scope) {
    this.serverURI = serverURI;
    this.credential = credential;
    this.token = token;
    this.scope = scope;
  }

  /**
   * Returns the URI of the OAuth server.
   *
   * @return the server URI
   */
  public String getServerURI() {
    return serverURI;
  }

  /**
   * Returns the credential used for authentication.
   *
   * @return the credential
   */
  public String getCredential() {
    return credential;
  }

  /**
   * Returns the access token obtained after authentication.
   *
   * @return the access token
   */
  public String getToken() {
    return token;
  }

  /**
   * Returns the scope of access granted by the OAuth token.
   *
   * @return the scope
   */
  public String getScope() {
    return scope;
  }
}
