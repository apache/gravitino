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
package org.apache.gravitino.dto.responses;

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

  /** Error codes for connect to catalog failed. */
  public static final int CONNECTION_FAILED_CODE = 1007;

  /** Error codes for forbidden operation. */
  public static final int FORBIDDEN_CODE = 1008;

  /** Error codes for operation on a no in use entity. */
  public static final int NOT_IN_USE_CODE = 1009;

  /** Error codes for drop an in use entity. */
  public static final int IN_USE_CODE = 1010;

  /** Error codes for invalid state. */
  public static final int UNKNOWN_ERROR_CODE = 1100;

  private ErrorConstants() {}
}
