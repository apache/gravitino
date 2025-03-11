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
package org.apache.gravitino.catalog.oceanbase.operation;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.TestJdbc;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.oceanbase.converter.OceanBaseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.oceanbase.converter.OceanBaseTypeConverter;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.OceanBaseContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestOceanBase extends TestJdbc {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  protected static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

  @BeforeAll
  public static void startup() {
    containerSuite.startOceanBaseContainer();

    DATA_SOURCE = DataSourceUtils.createDataSource(getOceanBaseCatalogProperties());

    DATABASE_OPERATIONS = new OceanBaseDatabaseOperations();
    TABLE_OPERATIONS = new OceanBaseTableOperations();
    JDBC_EXCEPTION_CONVERTER = new JdbcExceptionConverter();
    DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        DATA_SOURCE,
        JDBC_EXCEPTION_CONVERTER,
        new OceanBaseTypeConverter(),
        new OceanBaseColumnDefaultValueConverter(),
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

  private static Map<String, String> getOceanBaseCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    OceanBaseContainer oceanBaseContainer = containerSuite.getOceanBaseContainer();

    String jdbcUrl = oceanBaseContainer.getJdbcUrl();

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), OceanBaseContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), OceanBaseContainer.PASSWORD);

    return catalogProperties;
  }
}
