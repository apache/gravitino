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

package org.apache.gravitino.flink.connector.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.iceberg.flink.FlinkCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies that {@link GravitinoIcebergCatalogFlink21#createInnerIcebergCatalog} correctly threads
 * the given catalog name and properties into the anonymous {@code CatalogFactory.Context} shim
 * required to work around Flink 2.x removing {@code FlinkCatalogFactory}'s 2-arg {@code
 * createCatalog(String, Map)} overload.
 */
class TestGravitinoIcebergCatalogFlink21 {

  @TempDir private Path warehouse;

  @Test
  void createInnerIcebergCatalogThreadsNameAndProperties() throws Exception {
    Map<String, String> icebergProperties =
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            "hadoop",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "file://" + warehouse);

    GravitinoIcebergCatalogFlink21 catalog =
        new GravitinoIcebergCatalogFlink21(
            "my_catalog",
            "default",
            mock(SchemaAndTablePropertiesConverter.class),
            mock(PartitionConverter.class),
            icebergProperties,
            icebergProperties);

    Object inner = catalog.createInnerIcebergCatalog("my_catalog", icebergProperties);

    assertInstanceOf(FlinkCatalog.class, inner);
    AbstractCatalog innerCatalog = (AbstractCatalog) inner;
    // The catalog name was threaded through Context.getName().
    assertEquals("my_catalog", innerCatalog.getName());

    // The warehouse property was threaded through Context.getOptions(): opening the catalog and
    // listing databases succeeds against the temp warehouse rather than failing with a
    // missing-warehouse / uninitialized-catalog error.
    innerCatalog.open();
    try {
      List<String> databases = innerCatalog.listDatabases();
      assertTrue(databases.isEmpty());
    } finally {
      innerCatalog.close();
    }
  }
}
