/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

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

  /**
   * The default name of the attribute that stores the authenticated principal in the request.
   *
   * <p>Refer to the style of `AuthenticationFilter#AuthenticatedRoleAttributeName` of Apache Pulsar
   */
  public static final String AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME =
      AuthConstants.class.getName() + "-principal";
}
