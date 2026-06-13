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

import java.util.Map;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that the /v1/config endpoint gates optional endpoints (like scan planning) based on the
 * catalog backend's capabilities.
 */
public class TestIcebergConfigEndpointGating extends IcebergTestBase {

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(
        IcebergConfigOperations.class,
        true,
        java.util.Collections.emptyList(),
        NoScanPlanWrapperManager::new);
  }

  @Test
  public void testScanPlanEndpointOmittedWhenNotSupported() {
    Response resp = getConfigClientBuilder().get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);

    boolean hasScanPlanEndpoint =
        response.endpoints().stream()
            .anyMatch(
                endpoint ->
                    "POST".equals(endpoint.httpMethod())
                        && endpoint.path().contains("tables/{table}/plan"));

    Assertions.assertFalse(
        hasScanPlanEndpoint,
        "Config response must NOT advertise scan plan endpoint for catalogs that do not support it");
  }

  @Test
  public void testCoreEndpointsAlwaysPresent() {
    Response resp = getConfigClientBuilder().get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    ConfigResponse response = resp.readEntity(ConfigResponse.class);

    // Core table endpoints must always be present
    boolean hasListTables =
        response.endpoints().stream()
            .anyMatch(
                endpoint ->
                    "GET".equals(endpoint.httpMethod())
                        && endpoint.path().contains("namespaces/{namespace}/tables"));
    Assertions.assertTrue(hasListTables, "Config must always advertise list tables endpoint");

    boolean hasLoadTable =
        response.endpoints().stream()
            .anyMatch(
                endpoint ->
                    "GET".equals(endpoint.httpMethod())
                        && endpoint.path().contains("tables/{table}"));
    Assertions.assertTrue(hasLoadTable, "Config must always advertise load table endpoint");
  }

  /** Wrapper that overrides {@code supportsScanPlanOperations()} to return false. */
  static class NoScanPlanCatalogWrapper extends CatalogWrapperForTest {
    public NoScanPlanCatalogWrapper(String catalogName, IcebergConfig icebergConfig) {
      super(catalogName, icebergConfig);
    }

    @Override
    public boolean supportsScanPlanOperations() {
      return false;
    }
  }

  /** Manager that creates {@link NoScanPlanCatalogWrapper} instances. */
  public static class NoScanPlanWrapperManager extends IcebergCatalogWrapperManager {
    public NoScanPlanWrapperManager(
        Map<String, String> properties,
        IcebergConfigProvider configProvider,
        boolean auxMode,
        String metalakeName) {
      super(properties, configProvider, auxMode, metalakeName);
    }

    @Override
    public CatalogWrapperForREST createCatalogWrapper(
        String catalogName, IcebergConfig icebergConfig) {
      return new NoScanPlanCatalogWrapper(catalogName, icebergConfig);
    }
  }
}
