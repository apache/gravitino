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
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.NoSuchTopicException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.NotFoundException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.exceptions.RESTException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TopicAlreadyExistsException;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import java.util.List;
import java.util.function.Consumer;

/**
 * Utility class providing error handling for REST requests and specific to Metalake errors.
 *
 * <p>It also includes utility methods to format error messages and convert stack traces to strings.
 */
public class ErrorHandlers {

  /**
   * Creates an error handler specific to Metalake operations.
   *
   * @return A Consumer representing the Metalake error handler.
   */
  public static Consumer<ErrorResponse> metalakeErrorHandler() {
    return MetalakeErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Catalog operations.
   *
   * @return A Consumer representing the Catalog error handler.
   */
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
   * Creates an error handler specific to Partition operations.
   *
   * @return A Consumer representing the Partition error handler.
   */
  public static Consumer<ErrorResponse> partitionErrorHandler() {
    return PartitionErrorHandler.INSTANCE;
  }

  /**
   * Creates a generic error handler for REST requests.
   *
   * @return A Consumer representing the generic REST error handler.
   */
  public static Consumer<ErrorResponse> restErrorHandler() {
    return RestErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to OAuth2 requests.
   *
   * @return A Consumer representing the OAuth2 error handler.
   */
  public static Consumer<ErrorResponse> oauthErrorHandler() {
    return OAuthErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Fileset operations.
   *
   * @return A Consumer representing the Fileset error handler.
   */
  public static Consumer<ErrorResponse> filesetErrorHandler() {
    return FilesetErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Topic operations.
   *
   * @return A Consumer representing the Topic error handler.
   */
  public static Consumer<ErrorResponse> topicErrorHandler() {
    return TopicErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to User operations.
   *
   * @return A Consumer representing the User error handler.
   */
  public static Consumer<ErrorResponse> userErrorHandler() {
    return UserErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Group operations.
   *
   * @return A Consumer representing the Group error handler.
   */
  public static Consumer<ErrorResponse> groupErrorHandler() {
    return GroupErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Role operations.
   *
   * @return A Consumer representing the Role error handler.
   */
  public static Consumer<ErrorResponse> roleErrorHandler() {
    return RoleErrorHandler.INSTANCE;
  }
  /**
   * Creates an error handler specific to permission operations.
   *
   * @return A Consumer representing the permission error handler.
   */
  public static Consumer<ErrorResponse> permissionOperationErrorHandler() {
    return PermissionOperationErrorHandler.INSTANCE;
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

  /** Error handler specific to Partition operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class PartitionErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new PartitionErrorHandler();

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

          } else if (errorResponse
              .getType()
              .equals(NoSuchPartitionException.class.getSimpleName())) {
            throw new NoSuchPartitionException(errorMessage);

          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new PartitionAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Table operations. */
  @SuppressWarnings("FormatStringAnnotation")
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
        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Schema operations. */
  @SuppressWarnings("FormatStringAnnotation")
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

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Catalog operations. */
  @SuppressWarnings("FormatStringAnnotation")
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

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Metalake operations. */
  @SuppressWarnings("FormatStringAnnotation")
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

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** * Error handler specific to OAuth2 requests. */
  public static class OAuthErrorHandler extends RestErrorHandler {
    private static final ErrorHandler INSTANCE = new OAuthErrorHandler();

    /**
     * Parses the error response from the server.
     *
     * @param code The response code indicating the error status.
     * @param json The JSON data representing the error response.
     * @param mapper The ObjectMapper used to deserialize the JSON data.
     * @return An ErrorResponse object representing the error response.
     */
    @Override
    public ErrorResponse parseResponse(int code, String json, ObjectMapper mapper) {
      try {
        OAuth2ErrorResponse response = mapper.readValue(json, OAuth2ErrorResponse.class);
        return ErrorResponse.oauthError(code, response.getType(), response.getMessage());
      } catch (Exception e) {
        String errorMsg =
            String.format(
                "Error code: %d, error message: %s, json string: %s", code, e.getMessage(), json);
        return ErrorResponse.unknownError(errorMsg);
      }
    }

    /**
     * Accepts the error response and throws an exception based on the error type.
     *
     * @param errorResponse the input argument
     */
    @Override
    public void accept(ErrorResponse errorResponse) {
      if (errorResponse.getType() != null) {
        switch (errorResponse.getType()) {
          case OAuth2ClientUtil.INVALID_CLIENT_ERROR:
            throw new UnauthorizedException(
                "Not authorized: %s: %s", errorResponse.getType(), errorResponse.getMessage());
          case OAuth2ClientUtil.INVALID_REQUEST_ERROR:
          case OAuth2ClientUtil.INVALID_GRANT_ERROR:
          case OAuth2ClientUtil.UNAUTHORIZED_CLIENT_ERROR:
          case OAuth2ClientUtil.UNSUPPORTED_GRANT_TYPE_ERROR:
          case OAuth2ClientUtil.INVALID_SCOPE_ERROR:
            throw new BadRequestException(
                "Malformed request: %s: %s", errorResponse.getType(), errorResponse.getMessage());
          default:
            super.accept(errorResponse);
        }
      }
      super.accept(errorResponse);
    }
  }

  /** Error handler specific to Fileset operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class FilesetErrorHandler extends RestErrorHandler {

    private static final FilesetErrorHandler INSTANCE = new FilesetErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchFilesetException.class.getSimpleName())) {
            throw new NoSuchFilesetException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new FilesetAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Topic operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class TopicErrorHandler extends RestErrorHandler {

    private static final TopicErrorHandler INSTANCE = new TopicErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchTopicException.class.getSimpleName())) {
            throw new NoSuchTopicException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new TopicAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to User operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class UserErrorHandler extends RestErrorHandler {

    private static final UserErrorHandler INSTANCE = new UserErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchUserException.class.getSimpleName())) {
            throw new NoSuchUserException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new UserAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Group operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class GroupErrorHandler extends RestErrorHandler {

    private static final GroupErrorHandler INSTANCE = new GroupErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchGroupException.class.getSimpleName())) {
            throw new NoSuchGroupException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new GroupAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Role operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class RoleErrorHandler extends RestErrorHandler {

    private static final RoleErrorHandler INSTANCE = new RoleErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchRoleException.class.getSimpleName())) {
            throw new NoSuchRoleException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new RoleAlreadyExistsException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Permission operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class PermissionOperationErrorHandler extends RestErrorHandler {

    private static final PermissionOperationErrorHandler INSTANCE =
        new PermissionOperationErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchUserException.class.getSimpleName())) {
            throw new NoSuchUserException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchGroupException.class.getSimpleName())) {
            throw new NoSuchGroupException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchRoleException.class.getSimpleName())) {
            throw new NoSuchRoleException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
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
        String errorMsg =
            String.format(
                "Error code: %d, error message: %s, json string: %s", code, e.getMessage(), json);
        return ErrorResponse.unknownError(errorMsg);
      }
    }

    @Override
    public void accept(ErrorResponse errorResponse) {
      throw new RESTException("Unable to process: %s", formatErrorMessage(errorResponse));
    }
  }
}
