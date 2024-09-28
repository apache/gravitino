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

package org.apache.gravitino.iceberg.common;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergConfig {
  @Test
  public void testLoadIcebergConfig() {
    Map<String, String> properties =
        ImmutableMap.of(JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(), "1000");

    IcebergConfig icebergConfig = new IcebergConfig();
    icebergConfig.loadFromMap(properties, k -> k.startsWith("gravitino."));
    Assertions.assertEquals(
        JettyServerConfig.WEBSERVER_HTTP_PORT.getDefaultValue(),
        icebergConfig.get(JettyServerConfig.WEBSERVER_HTTP_PORT));

    IcebergConfig icebergRESTConfig2 = new IcebergConfig(properties);
    Assertions.assertEquals(1000, icebergRESTConfig2.get(JettyServerConfig.WEBSERVER_HTTP_PORT));
  }

  @Test
  public void testIcebergHttpPort() {
    Map<String, String> properties = ImmutableMap.of();
    IcebergConfig icebergConfig = new IcebergConfig(properties);
    JettyServerConfig jettyServerConfig = JettyServerConfig.fromConfig(icebergConfig);
    Assertions.assertEquals(
        IcebergConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTP_PORT, jettyServerConfig.getHttpPort());
    Assertions.assertEquals(
        IcebergConfig.DEFAULT_ICEBERG_REST_SERVICE_HTTPS_PORT, jettyServerConfig.getHttpsPort());

    properties =
        ImmutableMap.of(
            JettyServerConfig.WEBSERVER_HTTP_PORT.getKey(),
            "1000",
            JettyServerConfig.WEBSERVER_HTTPS_PORT.getKey(),
            "1001");
    icebergConfig = new IcebergConfig(properties);
    jettyServerConfig = JettyServerConfig.fromConfig(icebergConfig);
    Assertions.assertEquals(1000, jettyServerConfig.getHttpPort());
    Assertions.assertEquals(1001, jettyServerConfig.getHttpsPort());
  }
}
