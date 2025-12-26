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
package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonHiveIT extends CatalogPaimonBaseIT {

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "hive";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-paimon/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
    URI =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.URI, URI);

    return catalogProperties;
  }

  @Test
  void testPaimonSchemaProperties() throws Catalog.DatabaseNotExistException {
    SupportsSchemas schemas = catalog.asSchemas();

    // create schema check.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    schemaProperties.put("key", "hive");
    Schema createdSchema =
        schemas.createSchema(schemaIdent.name(), schema_comment, schemaProperties);
    Assertions.assertEquals(createdSchema.properties().get("key"), "hive");

    // load schema check.
    Schema schema = schemas.loadSchema(schemaIdent.name());
    Assertions.assertEquals(schema.properties().get("key"), "hive");
    Map<String, String> loadedProps = paimonCatalog.getDatabase(schemaIdent.name()).options();
    Assertions.assertEquals(loadedProps.get("key"), "hive");
  }
}
