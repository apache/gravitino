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
package org.apache.gravitino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.IcebergRESTServiceResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergRestServiceUri extends TestBase {

  private static final String ICEBERG_REST_PATH = withSlash("api/system/iceberg-rest");

  @Test
  public void testIcebergRestServiceUriPresent() throws JsonProcessingException {
    IcebergRESTServiceResponse resp =
        new IcebergRESTServiceResponse("http://irc-host:9001/iceberg");
    buildMockResource(
        Method.GET,
        ICEBERG_REST_PATH,
        ImmutableMap.of("metalake", "test"),
        null,
        resp,
        HttpStatus.SC_OK);

    Optional<String> uri = client.icebergRestServiceUri("test");

    Assertions.assertTrue(uri.isPresent());
    Assertions.assertEquals("http://irc-host:9001/iceberg", uri.get());
  }

  @Test
  public void testIcebergRestServiceUriAbsent() throws JsonProcessingException {
    IcebergRESTServiceResponse resp = new IcebergRESTServiceResponse(null);
    buildMockResource(
        Method.GET,
        ICEBERG_REST_PATH,
        ImmutableMap.of("metalake", "test"),
        null,
        resp,
        HttpStatus.SC_OK);

    Optional<String> uri = client.icebergRestServiceUri("test");

    Assertions.assertFalse(uri.isPresent());
  }

  @Test
  public void testIcebergRestServiceUriPropagatesServerError() throws JsonProcessingException {
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(
        Method.GET,
        ICEBERG_REST_PATH,
        ImmutableMap.of("metalake", "test"),
        null,
        errResp,
        HttpStatus.SC_INTERNAL_SERVER_ERROR);

    Assertions.assertThrows(RuntimeException.class, () -> client.icebergRestServiceUri("test"));
  }
}
