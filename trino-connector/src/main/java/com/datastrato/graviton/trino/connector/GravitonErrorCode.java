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
  GRAVITON_UNSUPPORTED_TRINO_VERSION(0, EXTERNAL),
  GRAVITON_METALAKE_NOT_EXISTS(1, EXTERNAL),
  GRAVITON_MISSING_CONFIG(2, EXTERNAL),
  GRAVITON_CREATE_INNER_CONNECTOR_FAILED(3, EXTERNAL),
  GRAVITON_UNSUPPORTED_CATALOG_PROVIDER(4, EXTERNAL),
  GRAVITON_CREATE_INTERNAL_CONNECTOR_ERROR(5, EXTERNAL),
  GRAVITON_SCHEMA_NOT_EXISTS(6, EXTERNAL),
  GRAVITON_CATALOG_NOT_EXISTS(7, EXTERNAL),
  GRAVITON_TABLE_NOT_EXISTS(8, EXTERNAL),
  GRAVITON_UNSUPPORTED_TRINO_DATATYPE(9, EXTERNAL),
  GRAVITON_UNSUPPORTED_GRAVITON_DATATYPE(10, EXTERNAL),
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
