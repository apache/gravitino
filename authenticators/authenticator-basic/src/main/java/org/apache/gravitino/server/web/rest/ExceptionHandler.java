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
package org.apache.gravitino.server.web.rest;

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
