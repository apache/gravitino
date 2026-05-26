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
package org.apache.gravitino.trino.connector.catalog.glue;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestGlueConnectorAdapter {

  @Test
  void testBuildConnectorConfig() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of("aws-region", "us-east-1", "warehouse", "s3://my-bucket/warehouse");
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "test_glue", "glue", "test catalog", Catalog.Type.RELATIONAL, properties);
    GlueConnectorAdapter adapter = new GlueConnectorAdapter();
    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    Assertions.assertEquals("glue", config.get("hive.metastore"));
    Assertions.assertEquals("us-east-1", config.get("hive.metastore.glue.region"));
    Assertions.assertEquals("allow-all", config.get("hive.security"));
    Assertions.assertEquals("true", config.get("fs.hadoop.enabled"));
    Assertions.assertEquals("true", config.get("hive.non-managed-table-writes-enabled"));
    Assertions.assertNull(config.get("hive.metastore.glue.catalogid"));
    Assertions.assertNull(config.get("hive.metastore.glue.aws-access-key"));
  }

  @Test
  void testBuildConnectorConfigWithOptionalProperties() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(
            "aws-region", "us-east-1",
            "warehouse", "s3://my-bucket/warehouse",
            "aws-glue-catalog-id", "123456789",
            "aws-access-key-id", "test-access-key",
            "aws-secret-access-key", "test-secret-key",
            "aws-glue-endpoint", "https://glue.custom.endpoint");
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            "test_glue", "glue", "test catalog", Catalog.Type.RELATIONAL, properties);
    GlueConnectorAdapter adapter = new GlueConnectorAdapter();
    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    Assertions.assertEquals("123456789", config.get("hive.metastore.glue.catalogid"));
    Assertions.assertEquals("test-access-key", config.get("hive.metastore.glue.aws-access-key"));
    Assertions.assertEquals("test-secret-key", config.get("hive.metastore.glue.aws-secret-key"));
    Assertions.assertEquals("test-access-key", config.get("hive.s3.aws-access-key"));
    Assertions.assertEquals("test-secret-key", config.get("hive.s3.aws-secret-key"));
    Assertions.assertEquals(
        "https://glue.custom.endpoint", config.get("hive.metastore.glue.endpoint-url"));
  }
}
