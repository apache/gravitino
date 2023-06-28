package com.datastrato.graviton.dto.responses;

public enum ErrorType {
  INVALID_ARGUMENTS(1001, "invalid_arguments"),
  INTERNAL_ERROR(1002, "internal_error"),
  NOT_FOUND(1003, "not_found"),
  UNAUTHORIZED(1004, "unauthorized"),
  FORBIDDEN(1005, "forbidden"),
  CONFLICT(1006, "conflict"),
  TOO_MANY_REQUESTS(1007, "too_many_requests"),
  SERVICE_UNAVAILABLE(1008, "service_unavailable"),
  UNKNOWN(1009, "unknown");

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
