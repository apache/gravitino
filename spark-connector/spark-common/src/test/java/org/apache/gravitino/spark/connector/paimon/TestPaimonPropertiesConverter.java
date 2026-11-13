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

package org.apache.gravitino.spark.connector.paimon;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPaimonPropertiesConverter {
  private final PaimonPropertiesConverter paimonPropertiesConverter =
      PaimonPropertiesConverter.getInstance();

  @Test
  void testCatalogPropertiesWithHiveBackend() {
    Map<String, String> properties =
        paimonPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND,
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND_HIVE,
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_URI,
                "hive-uri",
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_WAREHOUSE,
                "hive-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            PaimonPropertiesConstants.PAIMON_CATALOG_METASTORE,
            PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_HIVE,
            PaimonPropertiesConstants.PAIMON_CATALOG_URI,
            "hive-uri",
            PaimonPropertiesConstants.PAIMON_CATALOG_WAREHOUSE,
            "hive-warehouse"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithJdbcBackend() {
    Map<String, String> properties =
        paimonPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND,
                PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_JDBC,
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_URI,
                "jdbc-uri",
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_WAREHOUSE,
                "jdbc-warehouse",
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_JDBC_USER,
                "user",
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_JDBC_PASSWORD,
                "passwd",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            PaimonPropertiesConstants.PAIMON_CATALOG_METASTORE,
            PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_JDBC,
            PaimonPropertiesConstants.PAIMON_CATALOG_URI,
            "jdbc-uri",
            PaimonPropertiesConstants.PAIMON_CATALOG_WAREHOUSE,
            "jdbc-warehouse",
            PaimonPropertiesConstants.PAIMON_CATALOG_JDBC_USER,
            "user",
            PaimonPropertiesConstants.PAIMON_CATALOG_JDBC_PASSWORD,
            "passwd"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithFilesystemBackend() {
    Map<String, String> properties =
        paimonPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND,
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND_FILESYSTEM,
                PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_WAREHOUSE,
                "filesystem-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            PaimonPropertiesConstants.PAIMON_CATALOG_METASTORE,
            PaimonPropertiesConstants.PAIMON_CATALOG_BACKEND_FILESYSTEM,
            PaimonPropertiesConstants.PAIMON_CATALOG_WAREHOUSE,
            "filesystem-warehouse"),
        properties);
  }

  @Test
  void testTablePropertiesKeepProvider() {
    // Without the Hive backend, the provider is passed on unchanged, as before.
    Assertions.assertEquals(
        ImmutableMap.of(TableCatalog.PROP_PROVIDER, "parquet", "k1", "v1"),
        paimonPropertiesConverter.toGravitinoTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "parquet", "k1", "v1")));
  }

  @Test
  void testHiveBackendPropertiesForPaimonTable() {
    // USING paimon, or no USING clause, keeps a Paimon table.
    Assertions.assertEquals(
        ImmutableMap.of("k1", "v1"),
        paimonPropertiesConverter.toHiveBackendTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "paimon", "k1", "v1")));
    Assertions.assertEquals(
        ImmutableMap.of("k1", "v1"),
        paimonPropertiesConverter.toHiveBackendTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "PAIMON", "k1", "v1")));
    Assertions.assertEquals(
        ImmutableMap.of("k1", "v1"),
        paimonPropertiesConverter.toHiveBackendTableProperties(ImmutableMap.of("k1", "v1")));
  }

  @Test
  void testHiveBackendPropertiesForFormatTable() {
    // A file format provider becomes a Paimon format table of that format.
    Assertions.assertEquals(
        ImmutableMap.of(
            PaimonPropertiesConstants.PAIMON_TABLE_TYPE,
            PaimonPropertiesConstants.PAIMON_TABLE_TYPE_FORMAT_TABLE,
            PaimonPropertiesConstants.PAIMON_FILE_FORMAT,
            "csv",
            "csv.field-delimiter",
            "\\t"),
        paimonPropertiesConverter.toHiveBackendTableProperties(
            ImmutableMap.of(TableCatalog.PROP_PROVIDER, "CSV", "csv.field-delimiter", "\\t")));
  }

  @Test
  void testHiveBackendPropertiesWithLocation() {
    // LOCATION is passed to Paimon as the table path; Spark's external flag is dropped.
    Assertions.assertEquals(
        ImmutableMap.of(
            PaimonPropertiesConstants.PAIMON_TABLE_TYPE,
            PaimonPropertiesConstants.PAIMON_TABLE_TYPE_FORMAT_TABLE,
            PaimonPropertiesConstants.PAIMON_FILE_FORMAT,
            "parquet",
            PaimonPropertiesConstants.PAIMON_TABLE_LOCATION,
            "hdfs://ns/data/t1"),
        paimonPropertiesConverter.toHiveBackendTableProperties(
            ImmutableMap.of(
                TableCatalog.PROP_PROVIDER,
                "parquet",
                TableCatalog.PROP_LOCATION,
                "hdfs://ns/data/t1",
                TableCatalog.PROP_EXTERNAL,
                "true")));
  }
}
