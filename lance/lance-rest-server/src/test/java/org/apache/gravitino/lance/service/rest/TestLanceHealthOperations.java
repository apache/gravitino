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
package org.apache.gravitino.lance.service.rest;

import javax.ws.rs.core.Response;
import org.apache.gravitino.dto.HealthCheckDTO;
import org.apache.gravitino.dto.responses.HealthResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLanceHealthOperations {

  @Test
  public void testLiveReturns200() {
    LanceHealthOperations ops = new LanceHealthOperations();
    Response resp = ops.live();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    HealthResponse body = (HealthResponse) resp.getEntity();
    Assertions.assertEquals(HealthCheckDTO.Status.UP, body.getStatus());
    Assertions.assertFalse(body.getChecks().isEmpty());
    Assertions.assertEquals("httpServer", body.getChecks().get(0).getName());
    Assertions.assertEquals(HealthCheckDTO.Status.UP, body.getChecks().get(0).getStatus());
  }
}
