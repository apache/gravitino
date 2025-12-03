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
package org.apache.gravitino.iceberg.server;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoIcebergRESTServer {

  @Test
  public void testValidationSkipsWhenAuthorizationDisabled() {
    GravitinoIcebergRESTServer.validateAuthorizationConfig(
        false, ImmutableMap.of(IcebergConfig.ICEBERG_CONFIG_PREFIX + "any", "value"));
  }

  @Test
  public void testValidationPassesWhenDynamicProviderConfigured() {
    String providerKey =
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER;
    GravitinoIcebergRESTServer.validateAuthorizationConfig(
        true,
        ImmutableMap.of(
            providerKey, IcebergConstants.DYNAMIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME));
  }

  @Test
  public void testValidationFailsForStaticProviderWithAuthorization() {
    String providerKey =
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.ICEBERG_REST_CATALOG_CONFIG_PROVIDER;
    IllegalArgumentException exception =
        Assertions.assertThrowsExactly(
            IllegalArgumentException.class,
            () ->
                GravitinoIcebergRESTServer.validateAuthorizationConfig(
                    true,
                    ImmutableMap.of(
                        providerKey,
                        IcebergConstants.STATIC_ICEBERG_CATALOG_CONFIG_PROVIDER_NAME)));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("gravitino.iceberg-rest.catalog-config-provider=dynamic-config-provider"));
  }
}
