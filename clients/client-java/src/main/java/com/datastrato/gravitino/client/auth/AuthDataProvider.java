/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client.auth;

import java.io.Closeable;

/** The provider of authentication data */
public interface AuthDataProvider extends Closeable {

  /**
   * Judge whether AuthDataProvider can provide token data.
   *
   * @return true if the AuthDataProvider can provide token data otherwise false.
   */
  default boolean hasTokenData() {
    return false;
  }

  /**
   * Acquire the data of token for authentication
   *
   * @return the token data is used for authentication
   */
  default byte[] getTokenData() {
    return null;
  }
}
