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
import static io.trino.spi.StandardErrorCode.CATALOG_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.exceptions.RESTException;

/** Gravitino error code. */
public enum GravitinoErrorCode implements ErrorCodeSupplier {
  /** Unsupported Trino version. */
  GRAVITINO_UNSUPPORTED_TRINO_VERSION(0, EXTERNAL),
  /** Metalake not exists. */
  GRAVITINO_METALAKE_NOT_EXISTS(1, EXTERNAL),
  /** Missing config. */
  GRAVITINO_MISSING_CONFIG(2, EXTERNAL),
  /** Create inner connector failed. */
  GRAVITINO_CREATE_INNER_CONNECTOR_FAILED(3, EXTERNAL),
  /** Unsupported catalog provider. */
  GRAVITINO_UNSUPPORTED_CATALOG_PROVIDER(4, EXTERNAL),
  /** Create internal connector error. */
  GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR(5, EXTERNAL),
  /** Schema not exists. */
  GRAVITINO_SCHEMA_NOT_EXISTS(6, EXTERNAL),
  /** Catalog not exists. */
  GRAVITINO_CATALOG_NOT_EXISTS(7, EXTERNAL),
  /** Table not exists. */
  GRAVITINO_TABLE_NOT_EXISTS(8, EXTERNAL),
  /** Unsupported Trino datatype. */
  GRAVITINO_UNSUPPORTED_TRINO_DATATYPE(9, EXTERNAL),
  /** Unsupported Gravitino datatype. */
  GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE(10, EXTERNAL),
  /** Unsupported operation. */
  GRAVITINO_UNSUPPORTED_OPERATION(11, EXTERNAL),
  /** Column not exists. */
  GRAVITINO_COLUMN_NOT_EXISTS(12, EXTERNAL),
  /** Schema already exists. */
  GRAVITINO_SCHEMA_ALREADY_EXISTS(13, EXTERNAL),
  /** Table already exists. */
  GRAVITINO_TABLE_ALREADY_EXISTS(14, EXTERNAL),
  /** Schema not empty. */
  GRAVITINO_SCHEMA_NOT_EMPTY(15, EXTERNAL),
  /** Illegal argument. */
  GRAVITINO_ILLEGAL_ARGUMENT(16, EXTERNAL),
  /** Inner connector exception. */
  GRAVITINO_INNER_CONNECTOR_EXCEPTION(17, EXTERNAL),
  /** Iceberg unsupported JDBC type. */
  GRAVITINO_ICEBERG_UNSUPPORTED_JDBC_TYPE(18, EXTERNAL),
  /** Missing required property. */
  GRAVITINO_MISSING_REQUIRED_PROPERTY(19, EXTERNAL),
  /** Catalog already exists. */
  GRAVITINO_CATALOG_ALREADY_EXISTS(20, EXTERNAL),
  /** Metalake already exists. */
  GRAVITINO_METALAKE_ALREADY_EXISTS(21, EXTERNAL),
  /** Operation failed. */
  GRAVITINO_OPERATION_FAILED(22, EXTERNAL),
  /** Runtime error. */
  GRAVITINO_RUNTIME_ERROR(23, EXTERNAL),
  /** Duplicated catalogs. */
  GRAVITINO_DUPLICATED_CATALOGS(24, EXTERNAL),
  /** Expression error. */
  GRAVITINO_EXPRESSION_ERROR(25, EXTERNAL),
  /** View not exists. */
  GRAVITINO_VIEW_NOT_EXISTS(26, EXTERNAL),
  /** View already exists. */
  GRAVITINO_VIEW_ALREADY_EXISTS(27, EXTERNAL),
  /** Failed to connect to the Gravitino server or to the underlying source of a catalog. */
  GRAVITINO_CONNECTION_FAILED(28, EXTERNAL);

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

  /**
   * Converts an exception into a Trino exception. See {@link #toTrinoException(Exception, String)}.
   *
   * @param e the exception to convert
   * @return the Trino exception to throw
   */
  public static TrinoException toTrinoException(Exception e) {
    return toTrinoException(e, null);
  }

  /**
   * Converts an exception into a Trino exception. A {@link TrinoException} is returned as is, so a
   * caller may route every failure of a guarded block through this method, including a {@link
   * TrinoException} it throws inside that block. An {@link ExecutionException} is classified by its
   * cause. {@link ForbiddenException}, {@link NotInUseException}, {@link IllegalArgumentException}
   * and {@link UnsupportedOperationException} map to {@code PERMISSION_DENIED}, {@code
   * CATALOG_UNAVAILABLE}, {@link #GRAVITINO_ILLEGAL_ARGUMENT} and {@link
   * #GRAVITINO_UNSUPPORTED_OPERATION}. A {@link ConnectionFailedException}, or a {@link
   * RESTException} caused by a network failure, maps to {@link #GRAVITINO_CONNECTION_FAILED}.
   * Anything else maps to {@code GENERIC_INTERNAL_ERROR}.
   *
   * @param exception the exception to convert
   * @param context describes the failed operation; it prefixes the message of an exception that
   *     maps to {@code GENERIC_INTERNAL_ERROR}, whose own message may not say what failed
   * @return the Trino exception to throw
   */
  public static TrinoException toTrinoException(Exception exception, @Nullable String context) {
    Exception e =
        exception instanceof ExecutionException && exception.getCause() instanceof Exception
            ? (Exception) exception.getCause()
            : exception;
    if (e instanceof TrinoException) {
      return (TrinoException) e;
    }
    if (e instanceof ForbiddenException) {
      return new TrinoException(PERMISSION_DENIED, firstLineMessage(e), e);
    }
    if (e instanceof NotInUseException) {
      return new TrinoException(CATALOG_UNAVAILABLE, firstLineMessage(e), e);
    }
    if (e instanceof ConnectionFailedException
        || (e instanceof RESTException && isNetworkFailure(e.getCause()))) {
      return new TrinoException(GRAVITINO_CONNECTION_FAILED, firstLineMessage(e), e);
    }
    if (e instanceof IllegalArgumentException) {
      return new TrinoException(GRAVITINO_ILLEGAL_ARGUMENT, firstLineMessage(e), e);
    }
    if (e instanceof UnsupportedOperationException) {
      return new TrinoException(GRAVITINO_UNSUPPORTED_OPERATION, firstLineMessage(e), e);
    }
    String message = firstLineMessage(e);
    return new TrinoException(
        GENERIC_INTERNAL_ERROR, context == null ? message : context + ": " + message, e);
  }

  // The REST client wraps every IOException in a RESTException, including JSON (de)serialization
  // failures, so only socket-level causes mean the server could not be reached.
  private static boolean isNetworkFailure(@Nullable Throwable cause) {
    return cause instanceof SocketException
        || cause instanceof UnknownHostException
        || cause instanceof InterruptedIOException;
  }

  // Keeps the first non-blank line of the message. A failure received over REST carries the server
  // stack trace after it, and other failures may add details there as well; the full text stays
  // available in the cause.
  private static String firstLineMessage(Exception e) {
    return StringUtils.isBlank(e.getMessage())
        ? e.toString()
        : e.getMessage().lines().filter(StringUtils::isNotBlank).findFirst().orElse(e.toString());
  }
}
