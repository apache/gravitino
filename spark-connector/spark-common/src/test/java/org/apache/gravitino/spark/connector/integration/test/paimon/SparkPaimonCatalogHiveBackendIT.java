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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.spark.connector.paimon.PaimonPropertiesConstants;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** This class uses Apache Paimon HiveCatalog for the backend catalog. */
@Tag("gravitino-docker-test")
public abstract class SparkPaimonCatalogHiveBackendIT extends SparkPaimonCatalogIT {

  private static final String DIRECT_CATALOG = "paimon_direct";

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND,
        PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_HIVE);
    catalogProperties.put(PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_URI, hiveMetastoreUri);
    catalogProperties.put(PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_WAREHOUSE, warehouse);
    return catalogProperties;
  }

  @Override
  protected Map<String, String> getExtraSparkConfigs() {
    // A Paimon SparkCatalog that talks to the same Hive metastore directly. It creates the format
    // tables that the Gravitino catalog is then expected to read, since creating a format table
    // through Gravitino is not supported.
    Map<String, String> configs = Maps.newHashMap(super.getExtraSparkConfigs());
    configs.put("spark.sql.catalog." + DIRECT_CATALOG, "org.apache.paimon.spark.SparkCatalog");
    configs.put("spark.sql.catalog." + DIRECT_CATALOG + ".metastore", "hive");
    configs.put("spark.sql.catalog." + DIRECT_CATALOG + ".uri", hiveMetastoreUri);
    configs.put("spark.sql.catalog." + DIRECT_CATALOG + ".warehouse", warehouse);
    return configs;
  }

  @Test
  @Override
  protected void testCreateAndLoadSchema() {
    // Same as the common test, except that the schema comment is not checked: Gravitino's Paimon
    // catalog does not store the comment of a new schema yet (#5719).
    String testDatabaseName = "t_create1";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName + " WITH DBPROPERTIES (ID=001);");
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertTrue(databaseMeta.containsKey("Location"));
    Assertions.assertTrue(databaseMeta.get("Properties").contains("(ID,001)"));

    testDatabaseName = "t_create2";
    dropDatabaseIfExists(testDatabaseName);
    String testDatabaseLocation = "/tmp/" + testDatabaseName;
    sql(
        String.format(
            "CREATE DATABASE %s COMMENT 'comment' LOCATION '%s' WITH DBPROPERTIES (ID=002);",
            testDatabaseName, testDatabaseLocation));
    databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertTrue(databaseMeta.get("Location").contains(testDatabaseLocation));
    Assertions.assertTrue(databaseMeta.get("Properties").contains("(ID,002)"));
  }

  @Test
  @Override
  protected void testAlterSchema() {
    String testDatabaseName = "t_alter";
    dropDatabaseIfExists(testDatabaseName);
    sql("CREATE DATABASE " + testDatabaseName + " WITH DBPROPERTIES (ID=001);");
    Map<String, String> databaseMeta = getDatabaseMetadata(testDatabaseName);
    Assertions.assertTrue(databaseMeta.get("Properties").contains("(ID,001)"));

    // Gravitino's Paimon catalog doesn't support altering a schema, whatever the backend.
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            sql(
                String.format(
                    "ALTER DATABASE %s SET DBPROPERTIES ('ID'='002')", testDatabaseName)));
  }

  @Test
  void testReadExternalCsvFormatTable() throws Exception {
    String tableName = "test_csv_format_table";
    String database = getDefaultDatabase();
    String location = warehouse + "/" + database + ".db/" + tableName + "_data";
    SparkSession spark = getSparkSession();
    spark.sql(String.format("DROP TABLE IF EXISTS %s.%s.%s", DIRECT_CATALOG, database, tableName));
    hdfs.delete(new Path(location), true);

    spark.sql(
        String.format(
            "CREATE TABLE %s.%s.%s (id INT, name STRING, dt STRING) USING csv "
                + "PARTITIONED BY (dt) LOCATION '%s'",
            DIRECT_CATALOG, database, tableName, location));
    spark.sql(
        String.format(
            "INSERT INTO %s.%s.%s VALUES (1, 'a', '20260101'), (2, 'b', '20260101'),"
                + " (3, 'c', '20260102')",
            DIRECT_CATALOG, database, tableName));

    // Read the format table through the Gravitino catalog.
    List<String> rows = getQueryData(String.format("SELECT * FROM %s ORDER BY id", tableName));
    Assertions.assertEquals(
        Arrays.asList("1,a,20260101", "2,b,20260101", "3,c,20260102"), rows);
    List<String> filtered =
        getQueryData(
            String.format("SELECT id FROM %s WHERE dt = '20260102' ORDER BY id", tableName));
    Assertions.assertEquals(Arrays.asList("3"), filtered);

    // Dropping an external format table through Gravitino keeps its files.
    sql(String.format("DROP TABLE %s", tableName));
    Assertions.assertFalse(tableExists(tableName));
    Assertions.assertTrue(hdfs.exists(new Path(location)));
    hdfs.delete(new Path(location), true);
  }
}
