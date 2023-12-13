/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.OAuth2ErrorResponse;
import com.datastrato.gravitino.exceptions.BadRequestException;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.exceptions.RESTException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class providing error handling for REST requests and specific to Metalake errors.
 *
 * <p>It also includes utility methods to format error messages and convert stack traces to strings.
 */
public class ErrorHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlers.class);

  /**
   * Creates an error handler specific to Metalake operations.
   *
   * @return A Consumer representing the Metalake error handler.
   */
  public static Consumer<ErrorResponse> metalakeErrorHandler() {
    return MetalakeErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> catalogErrorHandler() {
    return CatalogErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Schema operations.
   *
   * @return A Consumer representing the Schema error handler.
   */
  public static Consumer<ErrorResponse> schemaErrorHandler() {
    return SchemaErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Table operations.
   *
   * @return A Consumer representing the Table error handler.
   */
  public static Consumer<ErrorResponse> tableErrorHandler() {
    return TableErrorHandler.INSTANCE;
  }

  /**
   * Creates a generic error handler for REST requests.
   *
   * @return A Consumer representing the generic REST error handler.
   */
  public static Consumer<ErrorResponse> restErrorHandler() {
    return RestErrorHandler.INSTANCE;
  }

  public static Consumer<ErrorResponse> oauthErrorHandler() {
    return OAuthErrorHandler.INSTANCE;
  }

  private ErrorHandlers() {}

  /**
   * Converts a list of stack trace elements to a formatted string with line breaks.
   *
   * @param stack The list of stack trace elements to be converted.
   * @return A formatted string representing the stack trace.
   */
  private static String getStackString(List<String> stack) {
    if (stack == null || stack.isEmpty()) {
      return "";
    } else {
      Joiner eol = Joiner.on("\n");
      return eol.join(stack);
    }
  }

  /**
   * Formats the error message along with the stack trace, if available.
   *
   * @param errorResponse The ErrorResponse object containing the error message and stack trace.
   * @return A formatted error message string.
   */
  private static String formatErrorMessage(ErrorResponse errorResponse) {
    String message = errorResponse.getMessage();
    String stack = getStackString(errorResponse.getStack());
    if (stack.isEmpty()) {
      return message;
    } else {
      return String.format("%s\n%s", message, stack);
    }
  }

  /** Error handler specific to Table operations. */
  private static class TableErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new TableErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchTableException.class.getSimpleName())) {
            throw new NoSuchTableException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new TableAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);
      }

      super.accept(errorResponse);
    }
  }

  /** Error handler specific to Schema operations. */
  private static class SchemaErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new SchemaErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchCatalogException.class.getSimpleName())) {
            throw new NoSuchCatalogException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new SchemaAlreadyExistsException(errorMessage);

        case ErrorConstants.NON_EMPTY_CODE:
          throw new NonEmptySchemaException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);
      }

      super.accept(errorResponse);
    }
  }

  /** Error handler specific to Catalog operations. */
  private static class CatalogErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new CatalogErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchCatalogException.class.getSimpleName())) {
            throw new NoSuchCatalogException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new CatalogAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);
      }

      super.accept(errorResponse);
    }
  }

  /** Error handler specific to Metalake operations. */
  private static class MetalakeErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new MetalakeErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          throw new NoSuchMetalakeException(errorMessage);

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new MetalakeAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);
      }

      super.accept(errorResponse);
    }
  }

  public static class OAuthErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new OAuthErrorHandler();

    @Override
    public ErrorResponse parseResponse(int code, String json, ObjectMapper mapper) {
      try {
        OAuth2ErrorResponse response = mapper.readValue(json, OAuth2ErrorResponse.class);
        return ErrorResponse.oauthError(code, response.getType(), response.getMessage());
      } catch (Exception x) {
        LOG.warn("Unable to parse error response", x);
      }
      String errorMsg = String.format("Error code: %d, message: %s", code, json);
      return ErrorResponse.unknownError(errorMsg);
    }

    @Override
    public void accept(ErrorResponse errorResponse) {
      if (errorResponse.getType() != null) {
        switch (errorResponse.getType()) {
          case OAuth2ClientUtil.INVALID_CLIENT_ERROR:
            throw new UnauthorizedException(
                String.format(
                    "Not authorized: %s: %s", errorResponse.getType(), errorResponse.getMessage()));
          case OAuth2ClientUtil.INVALID_REQUEST_ERROR:
          case OAuth2ClientUtil.INVALID_GRANT_ERROR:
          case OAuth2ClientUtil.UNAUTHORIZED_CLIENT_ERROR:
          case OAuth2ClientUtil.UNSUPPORTED_GRANT_TYPE_ERROR:
          case OAuth2ClientUtil.INVALID_SCOPE_ERROR:
            throw new BadRequestException(
                String.format(
                    "Malformed request: %s: %s",
                    errorResponse.getType(), errorResponse.getMessage()));
        }
      }
      super.accept(errorResponse);
    }
  }

  /** Generic error handler for REST requests. */
  private static class RestErrorHandler extends ErrorHandler {
    private static final ErrorHandler INSTANCE = new RestErrorHandler();

    @Override
    public ErrorResponse parseResponse(int code, String json, ObjectMapper mapper) {
      try {
        return mapper.readValue(json, ErrorResponse.class);
      } catch (Exception e) {
        LOG.warn("Failed to parse response: {}", json, e);
      }

      String errorMsg = String.format("Error code: %d, message: %s", code, json);
      return ErrorResponse.unknownError(errorMsg);
    }

    @Override
    public void accept(ErrorResponse errorResponse) {
      throw new RESTException("Unable to process: %s", formatErrorMessage(errorResponse));
    }
  }
}
