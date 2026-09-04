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
package org.apache.gravitino.server.web.mapper;

import javax.annotation.Priority;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.server.web.Utils;

/**
 * NotFoundExceptionMapper returns a structured JSON error body when Jersey cannot match a request
 * URI to any resource method at all, instead of letting the servlet container fall back to Jetty's
 * default HTML error page.
 */
@Priority(1)
public class NotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {

  @Override
  public Response toResponse(NotFoundException exception) {
    String message =
        StringUtils.isBlank(exception.getMessage())
            ? "Requested resource not found"
            : exception.getMessage();
    return Utils.notFound(NotFoundException.class.getSimpleName(), message);
  }
}
