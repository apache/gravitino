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
package org.apache.gravitino.trino.connector.catalog.jdbc.trino;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTrinoClusterCatalogPropertyConverter {

  @Test
  public void testBuildTrinoClusterConnectorProperties() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("jdbc-url", "jdbc:mysql://localhost:5432/test")
            .put("jdbc-user", "test")
            .put("jdbc-password", "test")
            .put("cloud.trino.connection-url", "jdbc:trino://localhost:8080")
            .put("cloud.trino.connection-user", "admin")
            .put("cloud.trino.connection-password", "123")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "postgresql", "test catalog", Catalog.Type.RELATIONAL, properties);
    TrinoClusterConnectorAdapter adapter = new TrinoClusterConnectorAdapter();

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test the converted properties, it's generating connector configuration by properties name
    // with the prefix 'cluster.'
    Assertions.assertEquals(
        config.get("connection-url"), "jdbc:trino://localhost:8080/" + mockCatalog.name());
    Assertions.assertEquals(config.get("connection-user"), "admin");
    Assertions.assertEquals(config.get("connection-password"), "123");
  }
}
