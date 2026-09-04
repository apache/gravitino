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

import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.glassfish.jersey.server.ParamException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestParamExceptionMapper {

  private final ParamExceptionMapper mapper = new ParamExceptionMapper();

  @Test
  public void testPathParamExceptionReturnsNotFound() {
    NumberFormatException cause = new NumberFormatException("For input string: \"abc\"");
    ParamException exception = new ParamException.PathParamException(cause, "version", null);

    Response response = mapper.toResponse(exception);
    ErrorResponse entity = (ErrorResponse) response.getEntity();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, entity.getCode());
    Assertions.assertTrue(entity.getMessage().contains("version"));
    Assertions.assertTrue(entity.getMessage().contains("For input string"));
  }

  @Test
  public void testQueryParamExceptionReturnsNotFound() {
    NumberFormatException cause = new NumberFormatException("For input string: \"xyz\"");
    ParamException exception = new ParamException.QueryParamException(cause, "limit", null);

    Response response = mapper.toResponse(exception);
    ErrorResponse entity = (ErrorResponse) response.getEntity();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, entity.getCode());
    Assertions.assertTrue(entity.getMessage().contains("limit"));
  }

  @Test
  public void testHeaderParamExceptionReturnsIllegalArguments() {
    NumberFormatException cause = new NumberFormatException("For input string: \"nope\"");
    ParamException exception = new ParamException.HeaderParamException(cause, "X-Count", null);

    Response response = mapper.toResponse(exception);
    ErrorResponse entity = (ErrorResponse) response.getEntity();

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, entity.getCode());
    Assertions.assertTrue(entity.getMessage().contains("X-Count"));
  }
}
