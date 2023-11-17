/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

public interface AuthConstants {
  String HTTP_HEADER_AUTHORIZATION = "Authorization";

  String HTTP_HEADER_AUTHORIZATION_BEARER = "Bearer ";

  String HTTP_HEADER_AUTHORIZATION_BASIC = "Basic ";

  String UNKNOWN_USER_NAME = "unknown";
}
