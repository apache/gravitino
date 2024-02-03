/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

import com.datastrato.gravitino.exceptions.RESTException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Represents an error response. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class ErrorResponse extends BaseResponse {

  @JsonProperty("type")
  private String type;

  @JsonProperty("message")
  private String message;

  @Nullable
  @JsonProperty("stack")
  private List<String> stack;

  private ErrorResponse(int code, String type, String message, List<String> stack) {
    super(code);
    this.type = type;
    this.message = message;
    this.stack = stack;
  }

  private ErrorResponse() {
    super();
    this.type = null;
    this.message = null;
    this.stack = null;
  }

  /** Validates the error response. */
  @Override
  public void validate() {
    super.validate();

    Preconditions.checkArgument(type != null && !type.isEmpty(), "type cannot be null or empty");
    Preconditions.checkArgument(
        message != null && !message.isEmpty(), "message cannot be null or empty");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ErrorResponse(")
        .append("code=")
        .append(super.getCode())
        .append(", type=")
        .append(type)
        .append(", message=")
        .append(message)
        .append(")");

    if (stack != null && !stack.isEmpty()) {
      for (String s : stack) {
        sb.append("\n\t").append(s);
      }
    }

    return sb.toString();
  }

  /**
   * Creates a new rest error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse restError(String message) {
    return new ErrorResponse(
        ErrorConstants.REST_ERROR_CODE, RESTException.class.getSimpleName(), message, null);
  }

  /**
   * Create a new illegal arguments error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse illegalArguments(String message) {
    return illegalArguments(message, null);
  }

  /**
   * Create a new illegal arguments error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @param throwable The throwable that caused the error.
   * @return The new instance.
   */
  public static ErrorResponse illegalArguments(String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        message,
        getStackTrace(throwable));
  }

  /**
   * Create a new not found error instance of {@link ErrorResponse}.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse notFound(String type, String message) {
    return notFound(type, message, null);
  }

  /**
   * Create a new not found error instance of {@link ErrorResponse}.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   * @param throwable The throwable that caused the error.
   * @return The new instance.
   */
  public static ErrorResponse notFound(String type, String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.NOT_FOUND_CODE, type, message, getStackTrace(throwable));
  }

  /**
   * Create a new internal error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse internalError(String message) {
    return internalError(message, null);
  }

  /**
   * Create a new internal error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @param throwable The throwable that caused the error.
   * @return The new instance.
   */
  public static ErrorResponse internalError(String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.INTERNAL_ERROR_CODE,
        RuntimeException.class.getSimpleName(),
        message,
        getStackTrace(throwable));
  }

  /**
   * Create a new already exists error instance of {@link ErrorResponse}.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse alreadyExists(String type, String message) {
    return alreadyExists(type, message, null);
  }

  /**
   * Create a new already exists error instance of {@link ErrorResponse}.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   * @param throwable The throwable that caused the error.
   * @return The new instance.
   */
  public static ErrorResponse alreadyExists(String type, String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.ALREADY_EXISTS_CODE, type, message, getStackTrace(throwable));
  }

  /**
   * Create a new non empty error instance of {@link ErrorResponse}.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse nonEmpty(String type, String message) {
    return nonEmpty(type, message, null);
  }

  /**
   * Create a new non empty error instance of {@link ErrorResponse}.
   *
   * @param type The type of the error.
   * @param message The message of the error.
   * @param throwable The throwable that caused the error.
   * @return The new instance.
   */
  public static ErrorResponse nonEmpty(String type, String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.NON_EMPTY_CODE, type, message, getStackTrace(throwable));
  }

  /**
   * Create a new unknown error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse unknownError(String message) {
    return new ErrorResponse(
        ErrorConstants.UNKNOWN_ERROR_CODE, RuntimeException.class.getSimpleName(), message, null);
  }

  /**
   * Create a new unknown error instance of {@link ErrorResponse}.
   *
   * @param code The code of the error.
   * @param type The type of the error.
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse oauthError(int code, String type, String message) {
    return new ErrorResponse(code, type, message, null);
  }

  /**
   * Create a new unsupported operation error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @return The new instance.
   */
  public static ErrorResponse unsupportedOperation(String message) {
    return unsupportedOperation(message, null);
  }

  /**
   * Create a new unsupported operation error instance of {@link ErrorResponse}.
   *
   * @param message The message of the error.
   * @param throwable The throwable that caused the error.
   * @return The new instance.
   */
  public static ErrorResponse unsupportedOperation(String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.UNSUPPORTED_OPERATION_CODE,
        UnsupportedOperationException.class.getSimpleName(),
        message,
        getStackTrace(throwable));
  }

  private static List<String> getStackTrace(Throwable throwable) {
    if (throwable == null) {
      return null;
    }

    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      throwable.printStackTrace(pw);
    }
    return Arrays.asList(sw.toString().split("\n"));
  }
}
