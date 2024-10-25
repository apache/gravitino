/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector;

import static io.trino.spi.ErrorType.EXTERNAL;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

public enum GravitinoErrorCode implements ErrorCodeSupplier {
  GRAVITINO_UNSUPPORTED_TRINO_VERSION(0, EXTERNAL),
  GRAVITINO_METALAKE_NOT_EXISTS(1, EXTERNAL),
  GRAVITINO_MISSING_CONFIG(2, EXTERNAL),
  GRAVITINO_CREATE_INNER_CONNECTOR_FAILED(3, EXTERNAL),
  GRAVITINO_UNSUPPORTED_CATALOG_PROVIDER(4, EXTERNAL),
  GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR(5, EXTERNAL),
  GRAVITINO_SCHEMA_NOT_EXISTS(6, EXTERNAL),
  GRAVITINO_CATALOG_NOT_EXISTS(7, EXTERNAL),
  GRAVITINO_TABLE_NOT_EXISTS(8, EXTERNAL),
  GRAVITINO_UNSUPPORTED_TRINO_DATATYPE(9, EXTERNAL),
  GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE(10, EXTERNAL),
  GRAVITINO_UNSUPPORTED_OPERATION(11, EXTERNAL),
  GRAVITINO_COLUMN_NOT_EXISTS(12, EXTERNAL),
  GRAVITINO_SCHEMA_ALREADY_EXISTS(13, EXTERNAL),
  GRAVITINO_TABLE_ALREADY_EXISTS(14, EXTERNAL),
  GRAVITINO_SCHEMA_NOT_EMPTY(15, EXTERNAL),
  GRAVITINO_ILLEGAL_ARGUMENT(16, EXTERNAL),
  GRAVITINO_INNER_CONNECTOR_EXCEPTION(17, EXTERNAL),
  GRAVITINO_ICEBERG_UNSUPPORTED_JDBC_TYPE(18, EXTERNAL),
  GRAVITINO_MISSING_REQUIRED_PROPERTY(19, EXTERNAL),
  GRAVITINO_CATALOG_ALREADY_EXISTS(20, EXTERNAL),
  GRAVITINO_METALAKE_ALREADY_EXISTS(21, EXTERNAL),
  GRAVITINO_OPERATION_FAILED(22, EXTERNAL),
  GRAVITINO_RUNTIME_ERROR(23, EXTERNAL),
  GRAVITINO_DUPLICATED_CATALOGS(24, EXTERNAL),
  GRAVITINO_EXPRESSION_ERROR(25, EXTERNAL);

  // suppress ImmutableEnumChecker because ErrorCode is outside the project.
  @SuppressWarnings("ImmutableEnumChecker")
  private final ErrorCode errorCode;

  GravitinoErrorCode(int code, ErrorType type) {
    errorCode = new ErrorCode(code + 0x0200_0000, name(), type);
  }

  @Override
  public ErrorCode toErrorCode() {
    return errorCode;
  }
}
