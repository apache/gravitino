/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.security;

public interface AuthConstants {
  String HTTP_HEADER_AUTHORIZATION = "Authorization";

  String AUTHORIZATION_BEARER_HEADER = "Bearer ";

  String AUTHORIZATION_BASIC_HEADER = "Basic ";

  String ANONYMOUS_USER = "anonymous";
}
