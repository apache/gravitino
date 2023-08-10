/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.client;

import com.datastrato.graviton.dto.responses.ErrorConstants;
import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.exceptions.NotFoundException;
import com.datastrato.graviton.exceptions.RESTException;
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
   * Creates a generic error handler for REST requests.
   *
   * @return A Consumer representing the generic REST error handler.
   */
  public static Consumer<ErrorResponse> restErrorHandler() {
    return RestErrorHandler.INSTANCE;
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

  /** Error handler specific to Catalog operations. */
  private static class CatalogErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new CatalogErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(formatErrorMessage(errorResponse));

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(formatErrorMessage(errorResponse));
          } else if (errorResponse.getType().equals(NoSuchCatalogException.class.getSimpleName())) {
            throw new NoSuchCatalogException(formatErrorMessage(errorResponse));
          } else {
            throw new NotFoundException(formatErrorMessage(errorResponse));
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new CatalogAlreadyExistsException(formatErrorMessage(errorResponse));

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(formatErrorMessage(errorResponse));
      }

      super.accept(errorResponse);
    }
  }

  /** Error handler specific to Metalake operations. */
  private static class MetalakeErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new MetalakeErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(formatErrorMessage(errorResponse));

        case ErrorConstants.NOT_FOUND_CODE:
          throw new NoSuchMetalakeException(formatErrorMessage(errorResponse));

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new MetalakeAlreadyExistsException(formatErrorMessage(errorResponse));

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(formatErrorMessage(errorResponse));
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
