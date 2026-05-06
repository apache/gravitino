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

package org.apache.gravitino.flink.connector.store;

import static org.apache.flink.table.factories.FactoryUtil.createCatalogStoreFactoryHelper;
import static org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactory.extractClientConfig;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoFlinkConfig {

  @Test
  void testDefaultClientConfig() {
    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "flink");
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://127.0.0.1:8090");
    Assertions.assertTrue(extractGrivitinoClientConfig(configuration).isEmpty());
  }

  @Test
  void testCustomClientConfig() {
    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", "flink");
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", "http://127.0.0.1:8090");
    configuration.setString(
        "table.catalog-store.gravitino.gravitino.client.socketTimeoutMs", "1000");
    configuration.setString(
        "table.catalog-store.gravitino.gravitino.client.connectionTimeoutMs", "2000");

    Map<String, String> clientConfig = extractGrivitinoClientConfig(configuration);
    Assertions.assertEquals(clientConfig.get("gravitino.client.socketTimeoutMs"), "1000");
    Assertions.assertEquals(clientConfig.get("gravitino.client.connectionTimeoutMs"), "2000");
  }

  private Map<String, String> extractGrivitinoClientConfig(Configuration configuration) {
    CatalogStoreFactory.Context context =
        TableFactoryUtil.buildCatalogStoreFactoryContext(
            configuration, this.getClass().getClassLoader());

    FactoryUtil.FactoryHelper<CatalogStoreFactory> factoryHelper =
        createCatalogStoreFactoryHelper(new GravitinoCatalogStoreFactory(), context);
    factoryHelper.validate();

    ReadableConfig options = factoryHelper.getOptions();
    return extractClientConfig(options);
  }
}
