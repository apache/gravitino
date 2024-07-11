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
package org.apache.gravitino.flink.connector.hive;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.flink.connector.DefaultTransformConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.TransformConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * The GravitinoHiveCatalog class is a implementation of the BaseCatalog class that is used to proxy
 * the HiveCatalog class.
 */
public class GravitinoHiveCatalog extends BaseCatalog {

  private HiveCatalog hiveCatalog;

  GravitinoHiveCatalog(
      String catalogName,
      String defaultDatabase,
      @Nullable HiveConf hiveConf,
      @Nullable String hiveVersion) {
    super(catalogName, defaultDatabase);
    this.hiveCatalog = new HiveCatalog(catalogName, defaultDatabase, hiveConf, hiveVersion);
  }

  @Override
  public void open() throws CatalogException {
    super.open();
    hiveCatalog.open();
  }

  @Override
  public void close() throws CatalogException {
    super.close();
    hiveCatalog.close();
  }

  public HiveConf getHiveConf() {
    return hiveCatalog.getHiveConf();
  }

  @Override
  public Optional<Factory> getFactory() {
    return hiveCatalog.getFactory();
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return HivePropertiesConverter.INSTANCE;
  }

  @Override
  protected TransformConverter getTransformConverter() {
    return DefaultTransformConverter.INSTANCE;
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath objectPath)
      throws TableNotExistException, CatalogException {
    return hiveCatalog.getTableStatistics(objectPath);
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    return hiveCatalog.getTableColumnStatistics(tablePath);
  }
}
