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

package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergPropertiesConverter {
  private static final IcebergPropertiesConverter CONVERTER = IcebergPropertiesConverter.INSTANCE;

  @Test
  void testCatalogPropertiesWithHiveBackend() {
    Map<String, String> properties =
        CONVERTER.toGravitinoCatalogProperties(
            Configuration.fromMap(
                ImmutableMap.of(
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                    "hive-uri",
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                    "hive-warehouse",
                    "key1",
                    "value1",
                    "flink.bypass.key2",
                    "value2")));

    Map<String, String> actual =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "hive-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "hive-warehouse",
            "flink.bypass.key1", // Automatically add 'flink.bypass.'
            "value1",
            "flink.bypass.key2",
            "value2");

    Assertions.assertEquals(actual, properties);

    Map<String, String> toFlinkProperties = CONVERTER.toFlinkCatalogProperties(actual);

    Assertions.assertEquals(
        ImmutableMap.of(
            CommonCatalogOptions.CATALOG_TYPE.key(),
            GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "hive-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "hive-warehouse",
            "key1", // When returning to Flink, prefix 'flink.bypass.' Automatically removed.
            "value1",
            "key2", // When returning to Flink, prefix 'flink.bypass.' Automatically removed.
            "value2"),
        toFlinkProperties);
  }

  @Test
  void testCatalogPropertiesWithRestBackend() {
    Map<String, String> properties =
        CONVERTER.toGravitinoCatalogProperties(
            Configuration.fromMap(
                ImmutableMap.of(
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                    IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                    "rest-uri",
                    IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                    "rest-warehouse",
                    "key1",
                    "value1",
                    "flink.bypass.key2",
                    "value2")));

    Map<String, String> actual =
        ImmutableMap.of(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "rest-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "rest-warehouse",
            "flink.bypass.key1", // Automatically add 'flink.bypass.'
            "value1",
            "flink.bypass.key2",
            "value2");

    Assertions.assertEquals(actual, properties);

    Map<String, String> toFlinkProperties = CONVERTER.toFlinkCatalogProperties(actual);

    Assertions.assertEquals(
        ImmutableMap.of(
            CommonCatalogOptions.CATALOG_TYPE.key(),
            GravitinoIcebergCatalogFactoryOptions.IDENTIFIER,
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "rest-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "rest-warehouse",
            "key1", // When returning to Flink, prefix 'flink.bypass.' Automatically removed.
            "value1",
            "key2", // When returning to Flink, prefix 'flink.bypass.' Automatically removed.
            "value2"),
        toFlinkProperties);
  }

  @Test
  void testRestAuthPropertiesOAuth2() {
    Map<String, String> authProperties =
        CONVERTER.toRestAuthProperties(
            ImmutableMap.of(
                GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.OAUTH2,
                GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI,
                "http://oauth-server",
                GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH,
                "/token",
                GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL,
                "client:secret",
                GravitinoCatalogStoreFactoryOptions.OAUTH2_SCOPE,
                "catalog"));

    Assertions.assertEquals(
        AuthProperties.AUTH_TYPE_OAUTH2, authProperties.get(AuthProperties.AUTH_TYPE));
    Assertions.assertEquals(
        "http://oauth-server/token", authProperties.get(OAuth2Properties.OAUTH2_SERVER_URI));
    Assertions.assertEquals("client:secret", authProperties.get(OAuth2Properties.CREDENTIAL));
    Assertions.assertEquals("catalog", authProperties.get(OAuth2Properties.SCOPE));
  }

  @Test
  void testRestAuthPropertiesOAuth2JoinsTokenEndpointSlashes() {
    Map<String, String> authProperties =
        CONVERTER.toRestAuthProperties(
            ImmutableMap.of(
                GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.OAUTH2,
                GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI,
                "http://oauth-server/",
                GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH,
                "/token",
                GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL,
                "client:secret"));

    Assertions.assertEquals(
        "http://oauth-server/token", authProperties.get(OAuth2Properties.OAUTH2_SERVER_URI));
  }

  @Test
  void testRestAuthPropertiesBasic() {
    Map<String, String> authProperties =
        CONVERTER.toRestAuthProperties(
            ImmutableMap.of(
                GravitinoCatalogStoreFactoryOptions.AUTH_TYPE,
                GravitinoCatalogStoreFactoryOptions.BASIC,
                GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME,
                "user",
                GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD,
                "password"));

    Assertions.assertEquals(
        AuthProperties.AUTH_TYPE_BASIC, authProperties.get(AuthProperties.AUTH_TYPE));
    Assertions.assertEquals("user", authProperties.get(AuthProperties.BASIC_USERNAME));
    Assertions.assertEquals("password", authProperties.get(AuthProperties.BASIC_PASSWORD));
  }

  @Test
  void testRestAuthPropertiesNoAuth() {
    Assertions.assertTrue(
        CONVERTER.toRestAuthProperties(ImmutableMap.of()).isEmpty(),
        "No auth type configured should yield no REST auth properties");
  }
}
