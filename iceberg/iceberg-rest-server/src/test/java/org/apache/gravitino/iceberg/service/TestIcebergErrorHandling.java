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
package org.apache.gravitino.iceberg.service;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests Iceberg HTTP responses for errors raised on the request path. */
public class TestIcebergErrorHandling extends JerseyTest {

  /** Simulates direct failures and failures inside the REST doAs boundary. */
  @Path("failure")
  public static class FailureResource {

    /**
     * Executes a request with an optional failure.
     *
     * @param mode whether to fail directly, within doAs, or return a successful response
     * @return the request response
     */
    @GET
    @Path("{mode}")
    public Response request(@PathParam("mode") String mode) {
      if ("healthy".equals(mode)) {
        return Response.ok().build();
      }
      Error failure = new NoClassDefFoundError("catalog class");
      failure.initCause(new ClassNotFoundException("missing dependency"));
      if ("direct".equals(mode)) {
        throw failure;
      }
      try {
        return PrincipalUtils.doAs(
            new UserPrincipal("test"),
            () -> {
              throw failure;
            });
      } catch (Exception e) {
        return IcebergExceptionMapper.toRESTResponse(e);
      }
    }
  }

  /**
   * Registers the same error and JSON mappers as the Iceberg REST service.
   *
   * @return the test application
   */
  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ResourceConfig()
        .register(FailureResource.class)
        .register(IcebergExceptionMapper.class)
        .register(IcebergObjectMapperProvider.class)
        .register(JacksonFeature.class);
  }

  /**
   * Verifies diagnostics survive both request paths and later requests can still succeed.
   *
   * @throws IOException if the JSON response cannot be parsed
   */
  @Test
  public void testErrorResponsesAndSubsequentRequests() throws IOException {
    for (String mode : new String[] {"direct", "do-as"}) {
      try (Response response =
          target("failure/" + mode).request(MediaType.APPLICATION_JSON_TYPE).get()) {
        Assertions.assertEquals(500, response.getStatus());
        Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());
        ErrorResponse error =
            IcebergObjectMapper.getInstance()
                .readValue(response.readEntity(String.class), ErrorResponse.class);
        Assertions.assertEquals(500, error.code());
        Assertions.assertEquals("NoClassDefFoundError", error.type());
        Assertions.assertEquals("catalog class", error.message());
        Assertions.assertTrue(
            String.join("\n", error.stack())
                .contains("Caused by: java.lang.ClassNotFoundException: missing dependency"));
      }
      try (Response response = target("failure/healthy").request().get()) {
        Assertions.assertEquals(200, response.getStatus());
      }
    }
  }
}
