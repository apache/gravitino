/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

public interface AuthConstants {
  String HTTP_HEADER_AUTHORIZATION = "Authorization";

  String AUTHORIZATION_BEARER_HEADER = "Bearer ";

  String AUTHORIZATION_BASIC_HEADER = "Basic ";

  String ANONYMOUS_USER = "anonymous";

  // Refer to the style of `AuthenticationFilter#AuthenticatedRoleAttributeName` of Apache Pulsar
  String AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME = AuthConstants.class.getName() + "-principal";
}
