/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server;

public class GravitinoServerException extends RuntimeException {

  public GravitinoServerException(String exception) {
    super(exception);
  }

  public GravitinoServerException(String exception, Throwable cause) {
    super(exception, cause);
  }

  public GravitinoServerException(Throwable cause) {
    super(cause);
  }
}
