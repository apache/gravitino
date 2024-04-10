/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.authentication;

import com.datastrato.gravitino.Config;
import java.security.Principal;

/** The interface provides authentication mechanism. */
public interface Authenticator {

  /**
   * Judge whether the data used to authenticate is from the token.
   *
   * @return true, if the data used to authenticate is from the token, Otherwise, it's false.
   */
  default boolean isDataFromToken() {
    return false;
  }

  /**
   * Use the token data to authenticate.
   *
   * @param tokenData The data is used for authentication
   * @return The identifier of user
   */
  default Principal authenticateToken(byte[] tokenData) {
    throw new UnsupportedOperationException(
        "Authenticator doesn't support to authenticate the data from the token");
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
