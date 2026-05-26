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
package org.apache.gravitino.idp.web;

import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.idp.exception.AlreadyExistsException;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Exception handlers for built-in IdP REST resources. */
public final class IdpRestExceptionHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(IdpRestExceptionHandlers.class);

  private IdpRestExceptionHandlers() {}

  /**
   * Handles built-in IdP user operation exceptions.
   *
   * @param op The operation type.
   * @param user The user name.
   * @param e The exception.
   * @return The REST response.
   */
  public static Response handleUserException(IdpOperationType op, String user, Exception e) {
    String formatted = StringUtils.isBlank(user) ? "" : " [" + user + "]";
    String errorMsg =
        String.format(
            "Failed to operate built-in IdP user %s operation [%s], reason [%s]",
            formatted, op.name(), e.getMessage());
    LOG.warn(errorMsg, e);
    return handleException(errorMsg, e);
  }

  /**
   * Handles built-in IdP group operation exceptions.
   *
   * @param op The operation type.
   * @param group The group name.
   * @param e The exception.
   * @return The REST response.
   */
  public static Response handleGroupException(IdpOperationType op, String group, Exception e) {
    String formatted = StringUtils.isBlank(group) ? "" : " [" + group + "]";
    String errorMsg =
        String.format(
            "Failed to operate built-in IdP group %s operation [%s], reason [%s]",
            formatted, op.name(), e.getMessage());
    LOG.warn(errorMsg, e);
    return handleException(errorMsg, e);
  }

  private static Response handleException(String errorMsg, Exception e) {
    if (e instanceof IllegalArgumentException) {
      return IdpRestUtils.illegalArguments(errorMsg, e);
    }
    if (e instanceof NotFoundException) {
      return IdpRestUtils.notFound(errorMsg, e);
    }
    if (e instanceof AlreadyExistsException) {
      return IdpRestUtils.alreadyExists(errorMsg, e);
    }
    if (e instanceof IllegalStateException || e instanceof UnsupportedOperationException) {
      return IdpRestUtils.unsupportedOperation(errorMsg, e);
    }
    return IdpRestUtils.internalError(errorMsg, e);
  }
}
