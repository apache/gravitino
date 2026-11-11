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

package org.apache.gravitino.spark.connector.integration.test.iceberg;

<<<<<<< HEAD:spark-connector/v3.4/spark/src/test/java/org/apache/gravitino/spark/connector/integration/test/iceberg/SparkIcebergCatalogRestBackendIT34.java
import org.junit.jupiter.api.condition.DisabledIf;

@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public class SparkIcebergCatalogRestBackendIT34 extends SparkIcebergCatalogRestBackendIT {}
=======
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.flink.connector.utils.CatalogCompatFlink22;

public class TestGravitinoJdbcCatalogFlink22 extends TestGravitinoJdbcCatalog {

  @Override
  protected CatalogCompat catalogCompat() {
    return CatalogCompatFlink22.INSTANCE;
  }

  @Override
  protected GravitinoJdbcCatalog createNativeCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    return new GravitinoJdbcCatalogFlink22(
        context, defaultDatabase, schemaAndTablePropertiesConverter, partitionConverter) {
      @Override
      protected Catalog catalog() {
        throw new NoSuchCatalogException("Catalog %s does not exist", context.getName());
      }
    };
  }
}
>>>>>>> f4056faaa ([#14801] fix(flink-connector): Honor bypass base-url and use jdbc-database for PostgreSQL connections (#14802)):flink-connector/v2.2/flink/src/test/java/org/apache/gravitino/flink/connector/jdbc/TestGravitinoJdbcCatalogFlink22.java
