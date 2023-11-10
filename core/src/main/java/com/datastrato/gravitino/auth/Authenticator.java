/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.auth;

import com.datastrato.gravitino.Config;

public interface Authenticator {

  /**
   * Judge whether the data used to authenticate is from the HTTP header.
   *
   * @return true, if the data used to authenticate is from the HTTP header, Otherwise, it's false.
   */
  default boolean isDataFromHTTP() {
    return false;
  }

  /**
   * Use the HTTP header data to authenticate.
   *
   * @param authData The data is used for authentication
   * @return The identifier of user
   */
  default String authenticateHTTPHeader(String authData) {
    throw new UnsupportedOperationException(
        "Don't support to authenticate the data from the HTTP header");
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
}
