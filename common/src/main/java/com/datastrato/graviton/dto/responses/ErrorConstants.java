package com.datastrato.graviton.dto.responses;

public class ErrorConstants {

  public static final int SYSTEM_ERROR_CODE = 1000;
  public static final String SYSTEM_ERROR_MSG = "system_error";

  public static final int INVALID_ARGUMENTS_CODE = 1001;
  public static final String INVALID_ARGUMENTS_MSG = "invalid_arguments";

  public static final int INTERNAL_ERROR_CODE = 1002;
  public static final String INTERNAL_ERROR_MSG = "internal_error";

  public static final int NOT_FOUND_CODE = 1003;
  public static final String NOT_FOUND_MSG = "not_found";

  public static final int ALREADY_EXISTS_CODE = 1004;
  public static final String ALREADY_EXISTS_MSG = "already_exists";

  public static final int UNAUTHORIZED_CODE = 1005;
  public static final String UNAUTHORIZED_MSG = "unauthorized";

  public static final int FORBIDDEN_CODE = 1006;
  public static final String FORBIDDEN_MSG = "forbidden";

  public static final int CONFLICT_CODE = 1007;
  public static final String CONFLICT_MSG = "conflict";

  public static final int TOO_MANY_REQUESTS_CODE = 1008;
  public static final String TOO_MANY_REQUESTS_MSG = "too_many_requests";

  public static final int SERVICE_UNAVAILABLE_CODE = 1009;
  public static final String SERVICE_UNAVAILABLE_MSG = "service_unavailable";

  public static final int UNKNOWN_CODE = 1100;
  public static final String UNKNOWN_MSG = "unknown";
}
