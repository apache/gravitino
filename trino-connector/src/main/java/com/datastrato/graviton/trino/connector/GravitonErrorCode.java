/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector;

import static io.trino.spi.ErrorType.EXTERNAL;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

public enum GravitonErrorCode implements ErrorCodeSupplier {
  GRAVITON_UNSUPPORTED_TRIO_VERSION(0, EXTERNAL),
  GRAVITON_NO_METALAKE_SELECTED(0, EXTERNAL),
  ;

  private final ErrorCode errorCode;

  GravitonErrorCode(int code, ErrorType type) {
    errorCode = new ErrorCode(code + 0x0200_0000, name(), type);
  }

  @Override
  public ErrorCode toErrorCode() {
    return errorCode;
  }
}
