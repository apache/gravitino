/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server;

public class GravitonServerException extends RuntimeException {

  public GravitonServerException(String exception) {
    super(exception);
  }

  public GravitonServerException(String exception, Throwable cause) {
    super(exception, cause);
  }

  public GravitonServerException(Throwable cause) {
    super(cause);
  }
}
