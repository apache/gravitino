/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.exceptions.RESTException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

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
  }

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

  public static ErrorResponse restError(String message) {
    return new ErrorResponse(
        ErrorConstants.REST_ERROR_CODE, RESTException.class.getSimpleName(), message, null);
  }

  public static ErrorResponse illegalArguments(String message) {
    return illegalArguments(message, null);
  }

  public static ErrorResponse illegalArguments(String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        message,
        getStackTrace(throwable));
  }

  public static ErrorResponse notFound(String type, String message) {
    return notFound(type, message, null);
  }

  public static ErrorResponse notFound(String type, String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.NOT_FOUND_CODE, type, message, getStackTrace(throwable));
  }

  public static ErrorResponse internalError(String message) {
    return internalError(message, null);
  }

  public static ErrorResponse internalError(String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.INTERNAL_ERROR_CODE,
        RuntimeException.class.getSimpleName(),
        message,
        getStackTrace(throwable));
  }

  public static ErrorResponse alreadyExists(String type, String message) {
    return alreadyExists(type, message, null);
  }

  public static ErrorResponse alreadyExists(String type, String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.ALREADY_EXISTS_CODE, type, message, getStackTrace(throwable));
  }

  public static ErrorResponse nonEmpty(String type, String message) {
    return nonEmpty(type, message, null);
  }

  public static ErrorResponse nonEmpty(String type, String message, Throwable throwable) {
    return new ErrorResponse(
        ErrorConstants.NON_EMPTY_CODE, type, message, getStackTrace(throwable));
  }

  public static ErrorResponse unknownError(String message) {
    return new ErrorResponse(
        ErrorConstants.UNKNOWN_ERROR_CODE, RuntimeException.class.getSimpleName(), message, null);
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
