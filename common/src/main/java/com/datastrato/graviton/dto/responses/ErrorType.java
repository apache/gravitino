package com.datastrato.graviton.dto.responses;

public enum ErrorType {
  SYSTEM_ERROR(ErrorConstants.SYSTEM_ERROR_CODE, ErrorConstants.SYSTEM_ERROR_MSG),
  INVALID_ARGUMENTS(ErrorConstants.INVALID_ARGUMENTS_CODE, ErrorConstants.INVALID_ARGUMENTS_MSG),
  INTERNAL_ERROR(ErrorConstants.INTERNAL_ERROR_CODE, ErrorConstants.INTERNAL_ERROR_MSG),
  NOT_FOUND(ErrorConstants.NOT_FOUND_CODE, ErrorConstants.NOT_FOUND_MSG),
  ALREADY_EXISTS(ErrorConstants.ALREADY_EXISTS_CODE, ErrorConstants.ALREADY_EXISTS_MSG),
  UNAUTHORIZED(ErrorConstants.UNAUTHORIZED_CODE, ErrorConstants.UNAUTHORIZED_MSG),
  FORBIDDEN(ErrorConstants.FORBIDDEN_CODE, ErrorConstants.FORBIDDEN_MSG),
  CONFLICT(ErrorConstants.CONFLICT_CODE, ErrorConstants.CONFLICT_MSG),
  TOO_MANY_REQUESTS(ErrorConstants.TOO_MANY_REQUESTS_CODE, ErrorConstants.TOO_MANY_REQUESTS_MSG),
  SERVICE_UNAVAILABLE(
      ErrorConstants.SERVICE_UNAVAILABLE_CODE, ErrorConstants.SERVICE_UNAVAILABLE_MSG),
  UNKNOWN(ErrorConstants.UNKNOWN_CODE, ErrorConstants.UNKNOWN_MSG);

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
