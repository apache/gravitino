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

import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestWebApplicationExceptionMapper {

  private final WebApplicationExceptionMapper mapper = new WebApplicationExceptionMapper();

  @Test
  public void testWrongHttpMethodReturnsJsonBody() {
    Response response = mapper.toResponse(new NotAllowedException("GET"));
    ErrorResponse entity = (ErrorResponse) response.getEntity();

    Assertions.assertEquals(
        Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), response.getStatus());
    Assertions.assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, entity.getCode());
    Assertions.assertFalse(entity.getMessage().isEmpty());
    Assertions.assertTrue(
        response.getHeaderString("Allow") != null && response.getHeaderString("Allow").contains("GET"));
  }

  @Test
  public void testUnsupportedMediaTypeReturnsJsonBody() {
    Response response = mapper.toResponse(new NotSupportedException("unsupported content type"));
    ErrorResponse entity = (ErrorResponse) response.getEntity();

    Assertions.assertEquals(
        Response.Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
    Assertions.assertEquals(ErrorConstants.REST_ERROR_CODE, entity.getCode());
    Assertions.assertEquals("unsupported content type", entity.getMessage());
  }

  @Test
  public void testNotAcceptableReturnsJsonBody() {
    Response response = mapper.toResponse(new NotAcceptableException());
    ErrorResponse entity = (ErrorResponse) response.getEntity();

    Assertions.assertEquals(Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus());
    Assertions.assertFalse(entity.getMessage().isEmpty());
  }
}
