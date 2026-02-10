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

package org.apache.gravitino.flink.connector.jdbc;

import java.util.Collections;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.UnsupportPartitionConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;

/**
 * Factory for creating instances of {@link GravitinoJdbcCatalog}. It will be created by SPI
 * discovery in Flink.
 */
public abstract class GravitinoJdbcCatalogFactory implements BaseCatalogFactory {

  @Override
  public org.apache.flink.table.catalog.Catalog createCatalog(Context context) {
    // FlinkJdbcCatalog does not support 'driver' as an option, but Gravitino JdbcCatalog requires
    // it.
    context.getOptions().remove(JdbcPropertiesConstants.FLINK_DRIVER);
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    String defaultDatabase =
        helper.getOptions().get(GravitinoJdbcCatalogFactoryOptions.DEFAULT_DATABASE);
    Preconditions.checkArgument(
        defaultDatabase != null,
        GravitinoJdbcCatalogFactoryOptions.DEFAULT_DATABASE.key() + " should not be null.");
    return new GravitinoJdbcCatalog(
        context, defaultDatabase, propertiesConverter(), partitionConverter());
  }

  @Override
  public Catalog.Type gravitinoCatalogType() {
    return Catalog.Type.RELATIONAL;
  }

  @Override
  public PartitionConverter partitionConverter() {
    return UnsupportPartitionConverter.INSTANCE;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }
}
