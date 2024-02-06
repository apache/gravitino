/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.responses;

/** Constants representing error codes for responses. */
public class ErrorConstants {

  /** Error codes for REST responses error. */
  public static final int REST_ERROR_CODE = 1000;

  /** Error codes for illegal arguments. */
  public static final int ILLEGAL_ARGUMENTS_CODE = 1001;

  /** Error codes for internal errors. */
  public static final int INTERNAL_ERROR_CODE = 1002;

  /** Error codes for not found. */
  public static final int NOT_FOUND_CODE = 1003;

  /** Error codes for already exists. */
  public static final int ALREADY_EXISTS_CODE = 1004;

  /** Error codes for non empty. */
  public static final int NON_EMPTY_CODE = 1005;

  /** Error codes for unsupported operation. */
  public static final int UNSUPPORTED_OPERATION_CODE = 1006;

  /** Error codes for invalid state. */
  public static final int UNKNOWN_ERROR_CODE = 1100;

  private ErrorConstants() {}
}
