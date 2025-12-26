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

@Tag("gravitino-docker-test")
public class CatalogPaimonFileSystemIT extends CatalogPaimonBaseIT {

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    TYPE = "filesystem";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-paimon/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, TYPE);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, WAREHOUSE);

    return catalogProperties;
  }

  @Test
  void testPaimonSchemaProperties() throws Catalog.DatabaseNotExistException {
    SupportsSchemas schemas = catalog.asSchemas();

    // create schema.
    String testSchemaName = GravitinoITUtils.genRandomName("test_schema_1");
    NameIdentifier schemaIdent = NameIdentifier.of(metalakeName, catalogName, testSchemaName);
    Map<String, String> schemaProperties = Maps.newHashMap();
    schemaProperties.put("key1", "val1");
    schemaProperties.put("key2", "val2");
    schemas.createSchema(schemaIdent.name(), schema_comment, schemaProperties);

    // load schema check, database properties is empty for Paimon FilesystemCatalog.
    Schema schema = schemas.loadSchema(schemaIdent.name());
    Assertions.assertTrue(schema.properties().isEmpty());
    Assertions.assertTrue(paimonCatalog.getDatabase(schemaIdent.name()).options().isEmpty());
  }
}
