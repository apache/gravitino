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
package org.apache.gravitino.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import java.util.List;
import java.util.function.Consumer;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.OAuth2ErrorResponse;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.IllegalMetadataObjectException;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.NonEmptyCatalogException;
import org.apache.gravitino.exceptions.NonEmptyMetalakeException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.exceptions.TopicAlreadyExistsException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;

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

  /**
   * Creates an error handler specific to Tag operations.
   *
   * @return A Consumer representing the Tag error handler.
   */
  public static Consumer<ErrorResponse> tagErrorHandler() {
    return TagErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to policy operations.
   *
   * @return A Consumer representing the policy error handler.
   */
  public static Consumer<ErrorResponse> policyErrorHandler() {
    return PolicyErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to credential operations.
   *
   * @return A Consumer representing the credential error handler.
   */
  public static Consumer<ErrorResponse> credentialErrorHandler() {
    return CredentialErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Owner operations.
   *
   * @return A Consumer representing the Owner error handler.
   */
  public static Consumer<ErrorResponse> ownerErrorHandler() {
    return OwnerErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Model operations.
   *
   * @return A Consumer representing the Model error handler.
   */
  public static Consumer<ErrorResponse> modelErrorHandler() {
    return ModelErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to job and job template operations.
   *
   * @return A Consumer representing the job error handler.
   */
  public static Consumer<ErrorResponse> jobErrorHandler() {
    return JobErrorHandler.INSTANCE;
  }

  /**
   * Creates an error handler specific to Statistics operations.
   *
   * @return A Consumer representing the Statistics error handler.
   */
  public static Consumer<ErrorResponse> statisticsErrorHandler() {
    return StatisticsErrorHandler.INSTANCE;
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
      return String.format("%s%n%s", message, stack);
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

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);

          } else {
            throw new NotInUseException(errorMessage);
          }

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

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);

          } else {
            throw new NotInUseException(errorMessage);
          }

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

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);

          } else {
            throw new NotInUseException(errorMessage);
          }

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

        case ErrorConstants.CONNECTION_FAILED_CODE:
          throw new ConnectionFailedException(errorMessage);

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

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        case ErrorConstants.IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogInUseException.class.getSimpleName())) {
            throw new CatalogInUseException(errorMessage);

          } else if (errorResponse.getType().equals(MetalakeInUseException.class.getSimpleName())) {
            throw new MetalakeInUseException(errorMessage);

          } else {
            throw new InUseException(errorMessage);
          }

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);

          } else {
            throw new NotInUseException(errorMessage);
          }

        case ErrorConstants.NON_EMPTY_CODE:
          throw new NonEmptyCatalogException(errorMessage);

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

        case ErrorConstants.IN_USE_CODE:
          throw new MetalakeInUseException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

        case ErrorConstants.NON_EMPTY_CODE:
          throw new NonEmptyMetalakeException(errorMessage);

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
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchCatalogException.class.getSimpleName())) {
            throw new NoSuchCatalogException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchFilesetException.class.getSimpleName())) {
            throw new NoSuchFilesetException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(NoSuchLocationNameException.class.getSimpleName())) {
            throw new NoSuchLocationNameException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new FilesetAlreadyExistsException(errorMessage);

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);

          } else {
            throw new NotInUseException(errorMessage);
          }

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

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);

          } else {
            throw new NotInUseException(errorMessage);
          }

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

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

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

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

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
          if (errorResponse.getType().equals(IllegalPrivilegeException.class.getSimpleName())) {
            throw new IllegalPrivilegeException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(IllegalMetadataObjectException.class.getSimpleName())) {
            throw new IllegalMetadataObjectException(errorMessage);
          } else {
            throw new IllegalArgumentException(errorMessage);
          }

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchRoleException.class.getSimpleName())) {
            throw new NoSuchRoleException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(NoSuchMetadataObjectException.class.getSimpleName())) {
            throw new NoSuchMetadataObjectException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new RoleAlreadyExistsException(errorMessage);

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

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
          if (errorResponse.getType().equals(IllegalPrivilegeException.class.getSimpleName())) {
            throw new IllegalPrivilegeException(errorMessage);
          } else if (errorResponse.getType().equals(IllegalRoleException.class.getSimpleName())) {
            throw new IllegalRoleException(errorMessage);
          } else {
            throw new IllegalArgumentException(errorMessage);
          }

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchUserException.class.getSimpleName())) {
            throw new NoSuchUserException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchGroupException.class.getSimpleName())) {
            throw new NoSuchGroupException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchRoleException.class.getSimpleName())) {
            throw new NoSuchRoleException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(NoSuchMetadataObjectException.class.getSimpleName())) {
            throw new NoSuchMetadataObjectException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Credential operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class CredentialErrorHandler extends RestErrorHandler {

    private static final CredentialErrorHandler INSTANCE = new CredentialErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Tag operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class TagErrorHandler extends RestErrorHandler {

    private static final TagErrorHandler INSTANCE = new TagErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchTagException.class.getSimpleName())) {
            throw new NoSuchTagException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          if (errorResponse.getType().equals(TagAlreadyExistsException.class.getSimpleName())) {
            throw new TagAlreadyExistsException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(TagAlreadyAssociatedException.class.getSimpleName())) {
            throw new TagAlreadyAssociatedException(errorMessage);
          } else {
            throw new AlreadyExistsException(errorMessage);
          }

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to policy operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class PolicyErrorHandler extends RestErrorHandler {

    private static final PolicyErrorHandler INSTANCE = new PolicyErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchPolicyException.class.getSimpleName())) {
            throw new NoSuchPolicyException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          if (errorResponse.getType().equals(PolicyAlreadyExistsException.class.getSimpleName())) {
            throw new PolicyAlreadyExistsException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(PolicyAlreadyAssociatedException.class.getSimpleName())) {
            throw new PolicyAlreadyAssociatedException(errorMessage);
          } else {
            throw new AlreadyExistsException(errorMessage);
          }

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Owner operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class OwnerErrorHandler extends RestErrorHandler {

    private static final OwnerErrorHandler INSTANCE = new OwnerErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMessage);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetadataObjectException.class.getSimpleName())) {
            throw new NoSuchMetadataObjectException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          throw new UnsupportedOperationException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          throw new MetalakeNotInUseException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Model operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class ModelErrorHandler extends RestErrorHandler {

    private static final ModelErrorHandler INSTANCE = new ModelErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMsg = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMsg);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMsg);
          } else if (errorResponse.getType().equals(NoSuchModelException.class.getSimpleName())) {
            throw new NoSuchModelException(errorMsg);
          } else if (errorResponse
              .getType()
              .equals(NoSuchModelVersionException.class.getSimpleName())) {
            throw new NoSuchModelVersionException(errorMsg);
          } else if (errorResponse
              .getType()
              .equals(NoSuchModelVersionURINameException.class.getSimpleName())) {
            throw new NoSuchModelVersionURINameException(errorMsg);
          } else {
            throw new NotFoundException(errorMsg);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          if (errorResponse.getType().equals(ModelAlreadyExistsException.class.getSimpleName())) {
            throw new ModelAlreadyExistsException(errorMsg);
          } else if (errorResponse
              .getType()
              .equals(ModelVersionAliasesAlreadyExistException.class.getSimpleName())) {
            throw new ModelVersionAliasesAlreadyExistException(errorMsg);
          } else {
            throw new AlreadyExistsException(errorMsg);
          }

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMsg);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMsg);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMsg);

          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMsg);

          } else {
            throw new NotInUseException(errorMsg);
          }

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to job and job template operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class JobErrorHandler extends RestErrorHandler {

    private static final JobErrorHandler INSTANCE = new JobErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMsg = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          throw new IllegalArgumentException(errorMsg);

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchMetalakeException.class.getSimpleName())) {
            throw new NoSuchMetalakeException(errorMsg);
          } else if (errorResponse
              .getType()
              .equals(NoSuchJobTemplateException.class.getSimpleName())) {
            throw new NoSuchJobTemplateException(errorMsg);
          } else if (errorResponse.getType().equals(NoSuchJobException.class.getSimpleName())) {
            throw new NoSuchJobException(errorMsg);
          } else {
            throw new NotFoundException(errorMsg);
          }

        case ErrorConstants.ALREADY_EXISTS_CODE:
          throw new JobTemplateAlreadyExistsException(errorMsg);

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMsg);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMsg);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMsg);

          } else {
            throw new NotInUseException(errorMsg);
          }

        case ErrorConstants.IN_USE_CODE:
          throw new InUseException(errorMsg);

        default:
          super.accept(errorResponse);
      }
    }
  }

  /** Error handler specific to Statistics operations. */
  @SuppressWarnings("FormatStringAnnotation")
  private static class StatisticsErrorHandler extends RestErrorHandler {

    private static final StatisticsErrorHandler INSTANCE = new StatisticsErrorHandler();

    @Override
    public void accept(ErrorResponse errorResponse) {
      String errorMessage = formatErrorMessage(errorResponse);

      switch (errorResponse.getCode()) {
        case ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
          if (errorResponse.getType().equals(IllegalStatisticNameException.class.getSimpleName())) {
            throw new IllegalStatisticNameException(errorMessage);
          } else {
            throw new IllegalArgumentException(errorMessage);
          }

        case ErrorConstants.NOT_FOUND_CODE:
          if (errorResponse.getType().equals(NoSuchSchemaException.class.getSimpleName())) {
            throw new NoSuchSchemaException(errorMessage);
          } else if (errorResponse.getType().equals(NoSuchTableException.class.getSimpleName())) {
            throw new NoSuchTableException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(NoSuchMetadataObjectException.class.getSimpleName())) {
            throw new NoSuchMetadataObjectException(errorMessage);
          } else {
            throw new NotFoundException(errorMessage);
          }

        case ErrorConstants.UNSUPPORTED_OPERATION_CODE:
          if (errorResponse
              .getType()
              .equals(UnmodifiableStatisticException.class.getSimpleName())) {
            throw new UnmodifiableStatisticException(errorMessage);
          } else {
            throw new UnsupportedOperationException(errorMessage);
          }

        case ErrorConstants.FORBIDDEN_CODE:
          throw new ForbiddenException(errorMessage);

        case ErrorConstants.INTERNAL_ERROR_CODE:
          throw new RuntimeException(errorMessage);

        case ErrorConstants.NOT_IN_USE_CODE:
          if (errorResponse.getType().equals(CatalogNotInUseException.class.getSimpleName())) {
            throw new CatalogNotInUseException(errorMessage);
          } else if (errorResponse
              .getType()
              .equals(MetalakeNotInUseException.class.getSimpleName())) {
            throw new MetalakeNotInUseException(errorMessage);
          } else {
            throw new NotInUseException(errorMessage);
          }

        case ErrorConstants.IN_USE_CODE:
          throw new InUseException(errorMessage);

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
      if (errorResponse.getCode() == ErrorConstants.FORBIDDEN_CODE) {
        throw new ForbiddenException("Forbidden error :%s", errorResponse.getMessage());
      }
      throw new RESTException("Unable to process: %s", formatErrorMessage(errorResponse));
    }
  }
}
