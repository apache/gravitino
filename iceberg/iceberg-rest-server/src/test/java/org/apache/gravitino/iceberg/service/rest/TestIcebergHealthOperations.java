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
package org.apache.gravitino.iceberg.service.rest;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergHealthOperations extends IcebergTestBase {

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(
        IcebergHealthOperations.class, false, java.util.Arrays.asList());
  }

  @Test
  public void testHealthCheck() {
    Response resp = target("/health").request(MediaType.APPLICATION_JSON_TYPE).get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    HealthResponse response = resp.readEntity(HealthResponse.class);
    Assertions.assertEquals("UP", response.getStatus());
  }

  public static class HealthResponse {
    private String status;

    public HealthResponse() {}

    public HealthResponse(String status) {
      this.status = status;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }
}
