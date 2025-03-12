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
package org.apache.gravitino.catalog.maxcompute.operation;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.catalog.maxcompute.converter.MaxComputeColumnDefaultValueConverter;
import org.apache.gravitino.catalog.maxcompute.converter.MaxComputeTypeConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class TestMaxCompute {

  protected static JdbcDatabaseOperations DATABASE_OPERATIONS;

  protected static JdbcTableOperations TABLE_OPERATIONS;

  protected static JdbcExceptionConverter JDBC_EXCEPTION_CONVERTER;

  protected static DataSource DATA_SOURCE;

  private static final String DRIVER_CLASS_NAME = "com.aliyun.odps.jdbc.OdpsDriver";
  private static String ALIBABA_CLOUD_ACCESS_KEY_ID = System.getenv("ALIBABA_CLOUD_ACCESS_KEY_ID");
  private static String ALIBABA_CLOUD_ACCESS_KEY_SECRET =
      System.getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET");
  private static String ALIBABA_CLOUD_JDBC_URL = System.getenv("ALIBABA_CLOUD_JDBC_URL");

  @BeforeAll
  public static void startup() {
    DATA_SOURCE = DataSourceUtils.createDataSource(getMaxComputeCatalogProperties());

    DATABASE_OPERATIONS = new MaxComputeSchemaOpeartions();
    TABLE_OPERATIONS = new MaxComputeTableOpeations();
    DATABASE_OPERATIONS.initialize(DATA_SOURCE, JDBC_EXCEPTION_CONVERTER, Collections.emptyMap());
    TABLE_OPERATIONS.initialize(
        DATA_SOURCE,
        JDBC_EXCEPTION_CONVERTER,
        new MaxComputeTypeConverter(),
        new MaxComputeColumnDefaultValueConverter(),
        Collections.emptyMap());
  }

  // Overwrite the stop method to close the data source and stop the container
  @AfterAll
  public static void stop() {
    DataSourceUtils.closeDataSource(DATA_SOURCE);
  }

  private static Map<String, String> getMaxComputeCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), ALIBABA_CLOUD_JDBC_URL);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), ALIBABA_CLOUD_ACCESS_KEY_ID);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), ALIBABA_CLOUD_ACCESS_KEY_SECRET);

    return catalogProperties;
  }
}
