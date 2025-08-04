/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.integration.test.util.IcebergRESTServerManager;
import org.apache.gravitino.iceberg.service.extension.HelloOperations;
import org.apache.gravitino.iceberg.service.extension.HelloResponse;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@TestInstance(Lifecycle.PER_CLASS)
// We couldn't add REST extension package jar in deploy mode, so just test embedded mode.
@EnabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public class TestIcebergExtendAPI {

  public static final Logger LOG = LoggerFactory.getLogger(TestIcebergExtendAPI.class);
  private IcebergRESTServerManager icebergRESTServerManager;
  private String uri;

  @BeforeAll
  void initIcebergTestEnv() throws Exception {
    this.icebergRESTServerManager = IcebergRESTServerManager.create();
    registerIcebergExtensionPackages();
    icebergRESTServerManager.startIcebergRESTServer();
    this.uri = String.format("http://127.0.0.1:%d/iceberg", getServerPort());
    LOG.info("Gravitino Iceberg REST server started, uri: {}", uri);
  }

  @AfterAll
  void stopIcebergTestEnv() {
    icebergRESTServerManager.stopIcebergRESTServer();
  }

  @Test
  void testExtendAPI() {
    RESTClient client =
        HTTPClient.builder(ImmutableMap.of()).uri(uri).withAuthSession(AuthSession.EMPTY).build();
    HelloResponse helloResponse =
        client.get(
            HelloOperations.HELLO_URI_PATH,
            HelloResponse.class,
            new HashMap<String, String>(),
            ErrorHandlers.defaultErrorHandler());
    Assertions.assertEquals(HelloOperations.HELLO_MSG, helloResponse.msg());
  }

  private void registerIcebergExtensionPackages() {
    Map<String, String> config =
        ImmutableMap.of(
            IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConfig.ICEBERG_EXTENSION_PACKAGES,
            HelloOperations.class.getPackage().getName());
    icebergRESTServerManager.registerCustomConfigs(config);
  }

  private int getServerPort() {
    JettyServerConfig jettyServerConfig =
        JettyServerConfig.fromConfig(
            icebergRESTServerManager.getServerConfig(), IcebergConfig.ICEBERG_CONFIG_PREFIX);
    return jettyServerConfig.getHttpPort();
  }
}
