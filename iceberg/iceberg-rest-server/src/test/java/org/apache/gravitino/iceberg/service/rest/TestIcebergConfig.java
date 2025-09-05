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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergConfig extends IcebergTestBase {

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(IcebergConfigOperations.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", IcebergRestTestUtil.PREFIX})
  public void testConfig(String prefix) {
    setUrlPathWithPrefix(prefix);
    Response resp = getConfigClientBuilder().get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);
    Assertions.assertEquals(0, response.defaults().size());
    Assertions.assertEquals(0, response.overrides().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", IcebergRestTestUtil.PREFIX})
  public void testConfigWithEmptyWarehouse(String prefix) {
    setUrlPathWithPrefix(prefix);
    Map<String, String> queryParams = ImmutableMap.of("warehouse", "");
    Response resp =
        getIcebergClientBuilder(IcebergRestTestUtil.CONFIG_PATH, Optional.of(queryParams)).get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);
    Assertions.assertEquals(0, response.defaults().size());
    Assertions.assertEquals(0, response.overrides().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", IcebergRestTestUtil.PREFIX})
  public void testConfigWithValidWarehouse(String prefix) {
    setUrlPathWithPrefix(prefix);
    String warehouseName = IcebergRestTestUtil.PREFIX;
    Map<String, String> queryParams = ImmutableMap.of("warehouse", warehouseName);
    Response resp =
        getIcebergClientBuilder(IcebergRestTestUtil.CONFIG_PATH, Optional.of(queryParams)).get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());
    ConfigResponse response = resp.readEntity(ConfigResponse.class);
    Map<String, String> expectedConfig =
        ImmutableMap.of(
            "prefix",
            warehouseName,
            IcebergConstants.IO_IMPL,
            "org.apache.iceberg.aws.s3.S3FileIO",
            IcebergConstants.ICEBERG_S3_ENDPOINT,
            "https://s3-endpoint.example.com",
            IcebergConstants.AWS_S3_REGION,
            "us-west-2",
            IcebergConstants.ICEBERG_OSS_ENDPOINT,
            "https://oss-endpoint.example.com",
            IcebergConstants.ICEBERG_S3_PATH_STYLE_ACCESS,
            "true");
    Assertions.assertEquals(expectedConfig, response.defaults());
    Assertions.assertEquals(0, response.overrides().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"invalid-catalog", "warehouse_123"})
  public void testConfigWithNonExistentWarehouses(String warehouse) {
    Map<String, String> queryParams = ImmutableMap.of("warehouse", warehouse);
    Response resp =
        getIcebergClientBuilder(IcebergRestTestUtil.CONFIG_PATH, Optional.of(queryParams)).get();
    Assertions.assertEquals(404, resp.getStatus());
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
