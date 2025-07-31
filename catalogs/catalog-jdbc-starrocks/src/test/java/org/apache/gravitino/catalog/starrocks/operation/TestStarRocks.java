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
package org.apache.gravitino.catalog.starrocks.operation;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksColumnDefaultValueConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksExceptionConverter;
import org.apache.gravitino.catalog.starrocks.converter.StarRocksTypeConverter;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksDatabaseOperations;
import org.apache.gravitino.catalog.starrocks.operations.StarRocksTableOperations;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.StarRocksContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestStarRocks extends TestJdbc {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  @BeforeAll
  public static void startup() {
    containerSuite.startStarRocksContainer();

    DATA_SOURCE = DataSourceUtils.createDataSource(getStarRocksCatalogProperties());

    DATABASE_OPERATIONS = new StarRocksDatabaseOperations();
    TABLE_OPERATIONS = new StarRocksTableOperations();
    JDBC_EXCEPTION_CONVERTER = new StarRocksExceptionConverter();
    DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        DATA_SOURCE,
        JDBC_EXCEPTION_CONVERTER,
        new StarRocksTypeConverter(),
        new StarRocksColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  // Overwrite the stop method to close the data source and stop the container
  @AfterAll
  public static void stop() {
    DataSourceUtils.closeDataSource(DATA_SOURCE);
    if (null != CONTAINER) {
      CONTAINER.stop();
    }
  }

  private static Map<String, String> getStarRocksCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    StarRocksContainer starRocksContainer = containerSuite.getStarRocksContainer();

    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            starRocksContainer.getContainerIpAddress(), StarRocksContainer.FE_MYSQL_PORT);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), StarRocksContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), StarRocksContainer.PASSWORD);

    return catalogProperties;
  }
}
