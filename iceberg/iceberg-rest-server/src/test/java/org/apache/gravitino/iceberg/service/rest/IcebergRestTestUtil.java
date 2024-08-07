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

package org.apache.gravitino.iceberg.service.rest;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergTableOps;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.logging.LoggingFeature.Verbosity;
import org.glassfish.jersey.server.ResourceConfig;

public class IcebergRestTestUtil {

  private static final String V_1 = "v1";
  public static final String PREFIX = "prefix_gravitino";
  public static final String CONFIG_PATH = V_1 + "/config";
  public static final String NAMESPACE_PATH = V_1 + "/namespaces";
  public static final String UPDATE_NAMESPACE_POSTFIX = "properties";
  public static final String TEST_NAMESPACE_NAME = "gravitino-test";
  public static final String TABLE_PATH = NAMESPACE_PATH + "/" + TEST_NAMESPACE_NAME + "/tables";
  public static final String RENAME_TABLE_PATH = V_1 + "/tables/rename";
  public static final String REPORT_METRICS_POSTFIX = "metrics";

  public static final boolean DEBUG_SERVER_LOG_ENABLED = true;

  public static ResourceConfig getIcebergResourceConfig(Class c) {
    return getIcebergResourceConfig(c, true);
  }

  public static ResourceConfig getIcebergResourceConfig(Class c, boolean bindIcebergTableOps) {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(c);
    resourceConfig.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    resourceConfig.register(IcebergExceptionMapper.class);

    if (DEBUG_SERVER_LOG_ENABLED) {
      resourceConfig.register(
          new LoggingFeature(
              Logger.getLogger(LoggingFeature.DEFAULT_LOGGER_NAME),
              Level.INFO,
              Verbosity.PAYLOAD_ANY,
              10000));
    }

    if (bindIcebergTableOps) {
      IcebergTableOps icebergTableOps = new IcebergTableOpsForTest();
      IcebergMetricsManager icebergMetricsManager = new IcebergMetricsManager(new IcebergConfig());
      resourceConfig.register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bind(icebergTableOps).to(IcebergTableOps.class).ranked(2);
              bind(icebergMetricsManager).to(IcebergMetricsManager.class).ranked(2);
            }
          });
    }
    return resourceConfig;
  }
}
