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
 *
 */
package org.apache.gravitino.lance.service;

import javax.ws.rs.core.Response;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ErrorResponse;

class TestLanceExceptionMapper {

  @Test
  void testRemoteAuthenticationAndAuthorizationErrorsKeepTheirStatus() {
    assertStatus(401, new UnauthorizedException("Missing caller credentials"));
    assertStatus(403, new ForbiddenException("Caller lacks permission"));
  }

  private void assertStatus(int status, Exception failure) {
    try (Response response = LanceExceptionMapper.toRESTResponse("catalog", failure)) {
      Assertions.assertEquals(status, response.getStatus());
      Assertions.assertEquals(
          failure.getMessage(), ((ErrorResponse) response.getEntity()).getError());
    }
  }
}
