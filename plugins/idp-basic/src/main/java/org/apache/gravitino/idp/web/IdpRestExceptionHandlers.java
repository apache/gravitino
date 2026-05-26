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
   * Handles built-in IdP REST operation exceptions.
   *
   * @param resourceType The resource type, such as {@code user} or {@code group}.
   * @param op The operation type.
   * @param name The resource name.
   * @param e The exception.
   * @return The REST response.
   */
  public static Response handleException(
      String resourceType, IdpOperationType op, String name, Exception e) {
    String formatted = StringUtils.isBlank(name) ? "" : " [" + name + "]";
    String errorMsg =
        String.format(
            "Failed to operate built-in IdP %s %s operation [%s], reason [%s]",
            resourceType, formatted, op.name(), e.getMessage());
    LOG.warn(errorMsg, e);
    return toResponse(errorMsg, e);
  }

  private static Response toResponse(String errorMsg, Exception e) {
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
