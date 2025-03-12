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

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.flink.connector.PropertiesConverter;

public abstract class JdbcPropertiesConverter implements PropertiesConverter {

  private final CatalogFactory.Context context;

  protected JdbcPropertiesConverter(CatalogFactory.Context context) {
    this.context = context;
  }

  @Override
  public Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoCatalogProperties =
        PropertiesConverter.super.toGravitinoCatalogProperties(flinkConf);
    gravitinoCatalogProperties.put(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER, driverName());
    return gravitinoCatalogProperties;
  }

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return JdbcPropertiesConstants.flinkToGravitinoMap.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    return JdbcPropertiesConstants.gravitinoToFlinkMap.get(configKey);
  }

  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> gravitinoProperties, ObjectPath tablePath) {
    Map<String, String> catalogOptions = context.getOptions();
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put(
        "url",
        catalogOptions.get(JdbcPropertiesConstants.FLINK_JDBC_URL)
            + "/"
            + tablePath.getDatabaseName());
    tableOptions.put("table-name", tablePath.getObjectName());
    tableOptions.put("username", catalogOptions.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    tableOptions.put("password", catalogOptions.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
    return tableOptions;
  }

  protected abstract String driverName();
}
