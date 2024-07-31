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

import java.util.Optional;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergConfig extends IcebergTestBase {

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(IcebergConfigOperations.class, false);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConfig(boolean withPrefix) {
    setUrlPathWithPrefix(withPrefix);
    Response resp = getConfigClientBuilder().get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);
    Assertions.assertEquals(0, response.defaults().size());
    Assertions.assertEquals(0, response.overrides().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"PREFIX", "", "\\\n\t\\\'", "\u0024", "\100", "[_~"})
  void testIcebergRestValidPrefix(String prefix) {
    String path = injectPrefixToPath(IcebergRestTestUtil.CONFIG_PATH, prefix);
    Response response = getIcebergClientBuilder(path, Optional.empty()).get();
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/", "hello/"})
  void testIcebergRestInvalidPrefix(String prefix) {
    String path = injectPrefixToPath(IcebergRestTestUtil.CONFIG_PATH, prefix);
    Response response = getIcebergClientBuilder(path, Optional.empty()).get();
    Assertions.assertEquals(500, response.getStatus());
  }
}
