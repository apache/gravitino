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
package org.apache.gravitino.spark.connector.integration.test.paimon;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.spark.connector.paimon.PaimonPropertiesConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** This class use Apache Paimon FilesystemCatalog for backend catalog. */
@Tag("gravitino-docker-test")
public abstract class SparkPaimonCatalogFilesystemBackendIT extends SparkPaimonCatalogIT {

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND,
        PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_FILESYSTEM);
    catalogProperties.put(PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_WAREHOUSE, warehouse);
    return catalogProperties;
  }

  @Test
  @Override
  protected void testCreateAndLoadSchema() {
    String testDatabaseName = "t_create1";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName + " WITH DBPROPERTIES (ID=001);");
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    // The database of the Paimon filesystem backend do not store any properties.
    Assertions.assertFalse(databaseMeta.containsKey("ID"));
    Assertions.assertFalse(databaseMeta.containsKey("comment"));
  }

  @Test
  @Override
  protected void testAlterSchema() {
    String testDatabaseName = "t_alter";
    dropDatabaseIfExists(testDatabaseName);
    sql(
        "CREATE DATABASE "
            + testDatabaseName
            + " COMMENT 'db comment' WITH DBPROPERTIES (ID=001);");
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    // The database of the Paimon filesystem backend do not store any properties.
    Assertions.assertFalse(databaseMeta.get("Properties").contains("(ID,001)"));
    Assertions.assertFalse(databaseMeta.containsKey("Comment"));

    // The Paimon filesystem backend do not support alter database operation.
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            sql(
                String.format(
                    "ALTER DATABASE %s SET DBPROPERTIES ('ID'='002')", testDatabaseName)));
  }
}
