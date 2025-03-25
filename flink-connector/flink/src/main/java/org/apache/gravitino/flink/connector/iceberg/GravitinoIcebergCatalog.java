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

import java.util.Map;
import java.util.Optional;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.FlinkCatalogFactory;

/** Gravitino Iceberg Catalog. */
public class GravitinoIcebergCatalog extends BaseCatalog {

  private final FlinkCatalog icebergCatalog;

  protected GravitinoIcebergCatalog(
      String catalogName,
      String defaultDatabase,
      PropertiesConverter propertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> flinkCatalogProperties) {
    super(
        catalogName,
        flinkCatalogProperties,
        defaultDatabase,
        propertiesConverter,
        partitionConverter);
    FlinkCatalogFactory flinkCatalogFactory = new FlinkCatalogFactory();
    this.icebergCatalog =
        (FlinkCatalog) flinkCatalogFactory.createCatalog(catalogName, flinkCatalogProperties);
  }

  @Override
  public Optional<Factory> getFactory() {
    return icebergCatalog.getFactory();
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return icebergCatalog;
  }
}
