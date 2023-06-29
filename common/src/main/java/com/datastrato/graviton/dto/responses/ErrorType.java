package com.datastrato.graviton.dto.responses;

public enum ErrorType {
  SYSTEM_ERROR(1000, "system_error"),
  INVALID_ARGUMENTS(1001, "invalid_arguments"),
  INTERNAL_ERROR(1002, "internal_error"),
  NOT_FOUND(1003, "not_found"),
  ALREADY_EXISTS(1004, "already_exists"),
  UNAUTHORIZED(1005, "unauthorized"),
  FORBIDDEN(1006, "forbidden"),
  CONFLICT(1007, "conflict"),
  TOO_MANY_REQUESTS(1008, "too_many_requests"),
  SERVICE_UNAVAILABLE(1009, "service_unavailable"),
  UNKNOWN(1100, "unknown");

  private final int errorCode;

  private final String errorType;

  ErrorType(int errorCode, String type) {
    this.errorCode = errorCode;
    this.errorType = type;
  }

  public int errorCode() {
    return errorCode;
  }

  public String errorType() {
    return errorType;
  }
}
