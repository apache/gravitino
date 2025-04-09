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

import static org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions.IDENTIFIER;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;
import org.apache.gravitino.flink.connector.utils.PropertyUtils;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Factory for creating instances of {@link GravitinoHiveCatalog}. It will be created by SPI
 * discovery in Flink.
 */
public class GravitinoHiveCatalogFactory implements BaseCatalogFactory {
  private HiveCatalogFactory hiveCatalogFactory;

  @Override
  public Catalog createCatalog(Context context) {
    this.hiveCatalogFactory = new HiveCatalogFactory();
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    helper.validateExcept(
        PropertyUtils.HIVE_PREFIX,
        PropertyUtils.HADOOP_PREFIX,
        PropertyUtils.DFS_PREFIX,
        PropertyUtils.FS_PREFIX);

    String hiveConfDir = helper.getOptions().get(HiveCatalogFactoryOptions.HIVE_CONF_DIR);
    String hadoopConfDir = helper.getOptions().get(HiveCatalogFactoryOptions.HADOOP_CONF_DIR);
    HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir);
    // Put the hadoop properties managed by Gravitino into the hiveConf
    PropertyUtils.getHadoopAndHiveProperties(context.getOptions()).forEach(hiveConf::set);
    return new GravitinoHiveCatalog(
        context.getName(),
        helper.getOptions().get(HiveCatalogFactoryOptions.DEFAULT_DATABASE),
        context.getOptions(),
        propertiesConverter(),
        partitionConverter(),
        hiveConf,
        helper.getOptions().get(HiveCatalogFactoryOptions.HIVE_VERSION));
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return ImmutableSet.<ConfigOption<?>>builder()
        .addAll(hiveCatalogFactory.requiredOptions())
        .add(GravitinoHiveCatalogFactoryOptions.HIVE_METASTORE_URIS)
        .build();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return hiveCatalogFactory.optionalOptions();
  }

  /**
   * Define gravitino catalog provider {@link org.apache.gravitino.CatalogProvider}.
   *
   * @return The requested gravitino catalog provider.
   */
  @Override
  public String gravitinoCatalogProvider() {
    return "hive";
  }

  /**
   * Define gravitino catalog type {@link org.apache.gravitino.Catalog.Type}.
   *
   * @return The requested gravitino catalog type.
   */
  @Override
  public org.apache.gravitino.Catalog.Type gravitinoCatalogType() {
    return org.apache.gravitino.Catalog.Type.RELATIONAL;
  }

  /**
   * Define properties converter {@link PropertiesConverter}.
   *
   * @return The requested property converter.
   */
  @Override
  public PropertiesConverter propertiesConverter() {
    return HivePropertiesConverter.INSTANCE;
  }

  /**
   * Define partition converter.
   *
   * @return The requested partition converter.
   */
  @Override
  public PartitionConverter partitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }
}
