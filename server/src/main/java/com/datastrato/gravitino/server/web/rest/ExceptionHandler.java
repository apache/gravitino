/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import javax.ws.rs.core.Response;

/**
 * The ExceptionHandler class is an abstract class specialized for handling Exceptions and returning
 * the appropriate Response.Subclasses of ExceptionHandler must implement {@link
 * ExceptionHandler#handle(OperationType, String, String, Exception)} to provide the {@link
 * Response} related to the exception.
 */
public abstract class ExceptionHandler {

  /**
   * Handles the exception and returns the appropriate Response. The implementation will use the
   * provided parameters to form a standard error response.
   *
   * @param op The operation that was attempted
   * @param object The object name that was being operated on
   * @param parent The parent object name that was being operated on
   * @param e The exception that was thrown
   * @return The Response object representing the error response
   */
  public abstract Response handle(OperationType op, String object, String parent, Exception e);
}
