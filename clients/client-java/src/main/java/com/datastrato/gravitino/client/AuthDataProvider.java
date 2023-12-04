/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import java.io.Closeable;

/** The provider of authentication data */
interface AuthDataProvider extends Closeable {

  /**
   * Judge whether AuthDataProvider can provide token data.
   *
   * @return true if the AuthDataProvider can provide token data otherwise false.
   */
  default boolean hasTokenData() {
    return false;
  }

  /**
   * Acquire the data of token for authentication. The client will set the token data as HTTP header
   * Authorization directly. So the return value should ensure token data contain the token header
   * (eg: Bearer, Basic) if necessary.
   *
   * @return the token data is used for authentication.
   */
  default byte[] getTokenData() {
    return null;
  }
}
