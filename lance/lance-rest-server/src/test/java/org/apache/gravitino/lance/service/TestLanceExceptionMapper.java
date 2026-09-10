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
package org.apache.gravitino.lance.service;

import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.namespace.model.ErrorResponse;

/** Tests for {@link LanceExceptionMapper}. */
public class TestLanceExceptionMapper extends JerseyTest {

  /** A resource that raises an error outside the operation-level exception handlers. */
  @Path("error")
  public static class ErrorResource {

    /**
     * Raises an assertion error.
     *
     * @return never returns normally
     */
    @GET
    public String fail() {
      AssertionError error = new AssertionError("assertion failure");
      error.initCause(new IllegalStateException("root cause"));
      throw error;
    }
  }

  /**
   * Configures the test resource and Lance exception mapper.
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
        .register(ErrorResource.class)
        .register(LanceExceptionMapper.class)
        .register(JacksonFeature.class);
  }

  /** Verifies that an uncaught error is converted to a Lance internal error response. */
  @Test
  public void testErrorResponse() {
    try (Response response = target("error").request(MediaType.APPLICATION_JSON_TYPE).get()) {
      Assertions.assertEquals(
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
      ErrorResponse entity = response.readEntity(ErrorResponse.class);
      Assertions.assertEquals("assertion failure", entity.getError());
      Assertions.assertEquals("", entity.getInstance());
      Assertions.assertTrue(
          entity.getDetail().contains("java.lang.AssertionError: assertion failure"));
      Assertions.assertTrue(
          entity.getDetail().contains("Caused by: java.lang.IllegalStateException: root cause"));
    }
  }
}
