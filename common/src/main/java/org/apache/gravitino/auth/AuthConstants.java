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

/** Constants used for authentication. */
public final class AuthConstants {

  private AuthConstants() {}

  /** The HTTP header used to pass the authentication token. */
  public static final String HTTP_HEADER_AUTHORIZATION = "Authorization";

  /** The name of BEARER header used to pass the authentication token. */
  public static final String AUTHORIZATION_BEARER_HEADER = "Bearer ";

  /** The name of BASIC header used to pass the authentication token. */
  public static final String AUTHORIZATION_BASIC_HEADER = "Basic ";

  /** The name of NEGOTIATE. */
  public static final String NEGOTIATE = "Negotiate";

  /** The value of NEGOTIATE header used to pass the authentication token. */
  public static final String AUTHORIZATION_NEGOTIATE_HEADER = NEGOTIATE + " ";

  /** The HTTP header used to pass the authentication token. */
  public static final String HTTP_CHALLENGE_HEADER = "WWW-Authenticate";

  /** The default username used for anonymous access. */
  public static final String ANONYMOUS_USER = "anonymous";

  /** OWNER. */
  public static final String OWNER = "OWNER";

  /** SELF authorization expression. */
  public static final String SELF = "SELF";

  /** deny. */
  public static final String DENY = "deny";

  /** allow. */
  public static final String ALLOW = "allow";

  /**
   * The default name of the attribute that stores the authenticated principal in the request.
   *
   * <p>Refer to the style of `AuthenticationFilter#AuthenticatedRoleAttributeName` of Apache Pulsar
   */
  public static final String AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME =
      AuthConstants.class.getName() + "-principal";
}
