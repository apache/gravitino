/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ErrorResponse;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.RegisterTableResponse;

class TestRegisterTableValidation {
  @Test
  void testValidLocationReachesBackend() {
    NamespaceWrapper backend = mock(NamespaceWrapper.class, RETURNS_DEEP_STUBS);
    RegisterTableRequest request = new RegisterTableRequest();
    request.setLocation("s3://bucket/existing.lance");
    when(backend
            .asTableOps()
            .registerTable(
                eq("cat$schema$table"),
                eq("create"),
                eq("$"),
                argThat(
                    properties -> "s3://bucket/existing.lance".equals(properties.get("location")))))
        .thenReturn(new RegisterTableResponse());
    try (Response response =
        new LanceTableOperations(backend).registerTable("cat$schema$table", "$", null, request)) {
      assertEquals(200, response.getStatus());
      verify(backend.asTableOps())
          .registerTable(
              eq("cat$schema$table"),
              eq("create"),
              eq("$"),
              argThat(
                  properties -> "s3://bucket/existing.lance".equals(properties.get("location"))));
    }
  }

  @Test
  void testMissingLocation() {
    NamespaceWrapper backend = mock(NamespaceWrapper.class);
    LanceTableOperations resource = new LanceTableOperations(backend);
    try (Response response =
        resource.registerTable("cat$schema$table", "$", null, new RegisterTableRequest())) {
      assertEquals(400, response.getStatus());
      assertEquals(13, ((ErrorResponse) response.getEntity()).getCode());
      verifyNoInteractions(backend);
    }
  }

  @Test
  void testMissingBody() {
    NamespaceWrapper backend = mock(NamespaceWrapper.class);
    try (Response response =
        new LanceTableOperations(backend).registerTable("cat$schema$table", "$", null, null)) {
      assertEquals(400, response.getStatus());
      verifyNoInteractions(backend);
    }
  }
}
