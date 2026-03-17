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

package org.apache.gravitino.flink.connector.utils;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestIcebergCatalogCompatUtils {

  @Test
  public void testCreateIcebergCatalog() {
    Map<String, String> icebergCatalogOptions =
        Map.of(
            FlinkCatalogFactory.ICEBERG_CATALOG_TYPE,
            CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
            "warehouse",
            "file:/tmp/gravitino-iceberg-warehouse");
    CatalogFactory.Context context = Mockito.mock(CatalogFactory.Context.class);
    Mockito.when(context.getName()).thenReturn("iceberg_catalog");
    Mockito.when(context.getOptions()).thenReturn(icebergCatalogOptions);
    Mockito.when(context.getConfiguration()).thenReturn(new Configuration());
    Mockito.when(context.getClassLoader())
        .thenReturn(Thread.currentThread().getContextClassLoader());

    AbstractCatalog catalog =
        IcebergCatalogCompatUtils.createIcebergCatalog(
            "iceberg_catalog", icebergCatalogOptions, context);

    Assertions.assertNotNull(catalog);
  }
}
