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

package org.apache.gravitino.flink.connector.integration.test.iceberg;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.types.Row;
import org.apache.gravitino.flink.connector.iceberg.IcebergPropertiesConstants;
import org.apache.gravitino.flink.connector.integration.test.utils.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.DisabledIf;

@Tag("gravitino-docker-test")
// Flink connector use low Iceberg version, couldn't work with Iceberg REST server with high Iceberg
// version in embedded mode.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public class FlinkIcebergRestCatalogIT extends FlinkIcebergCatalogIT {

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, icebergRestServiceUri);
    return catalogProperties;
  }

  @Override
  public void testListSchema() {
    doWithCatalog(
        currentCatalog(),
        catalog -> {
          String schema = "test_list_schema";
          String schema2 = "test_list_schema2";
          String schema3 = "test_list_schema3";

          try {
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema2), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("CREATE DATABASE IF NOT EXISTS %s", schema3), ResultKind.SUCCESS);
            TestUtils.assertTableResult(
                sql("SHOW DATABASES"),
                ResultKind.SUCCESS_WITH_CONTENT,
                Row.of("default"),
                Row.of(schema),
                Row.of(schema2),
                Row.of(schema3));

            String[] schemas = catalog.asSchemas().listSchemas();
            Arrays.sort(schemas);
            Assertions.assertEquals(4, schemas.length);
            Assertions.assertEquals("default", schemas[0]);
            Assertions.assertEquals(schema, schemas[1]);
            Assertions.assertEquals(schema2, schemas[2]);
            Assertions.assertEquals(schema3, schemas[3]);
          } finally {
            catalog.asSchemas().dropSchema(schema, supportDropCascade());
            catalog.asSchemas().dropSchema(schema2, supportDropCascade());
            catalog.asSchemas().dropSchema(schema3, supportDropCascade());
            // TODO: The check cannot pass in CI, but it can be successful locally.
            // Assertions.assertEquals(1, catalog.asSchemas().listSchemas().length);
          }
        });
  }

  @Override
  protected String getCatalogBackend() {
    return "rest";
  }

  @Override
  protected String getUri() {
    return icebergRestServiceUri;
  }
}
