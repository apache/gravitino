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
package org.apache.gravitino.flink.connector.integration.test.catalog;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.gateway.SqlGateway;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.session.SessionManager;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rest.RESTUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoCatalogManagerIT extends BaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManagerIT.class);

  protected static final String GRAVITINO_METALAKE = "flink";

  protected static GravitinoMetalake metalake;

  protected static TableEnvironment tableEnv;

  protected static SqlGateway sqlGateway;

  private static String gravitinoUri = "http://127.0.0.1:8090";

  private static String sqlGatewayHost = "localhost";

  private static int sqlGatewayPort;

  private static String sqlGatewayRestUri;

  @BeforeAll
  void startUp() throws Exception {
    // Start Gravitino server
    super.startIntegrationTest();
    initGravitinoEnv();
    initMetalake();
    initFlinkEnv();
    LOG.info("Startup Flink env successfully, Gravitino uri: {}.", gravitinoUri);
  }

  @AfterAll
  void stop() throws Exception {
    stopFlinkEnv();
    super.stopIntegrationTest();
    LOG.info("Stop Flink env successfully.");
  }

  private void initGravitinoEnv() {
    // Gravitino server is already started by AbstractIT, just construct gravitinoUri
    int gravitinoPort = getGravitinoServerPort();
    gravitinoUri = String.format("http://127.0.0.1:%d", gravitinoPort);
  }

  private void initMetalake() {
    metalake = client.createMetalake(GRAVITINO_METALAKE, "", Collections.emptyMap());
  }

  private static void initFlinkEnv() throws Exception {
    sqlGatewayPort = RESTUtils.findAvailablePort(3000, 4000);
    sqlGatewayRestUri = String.format("http://%s:%d", sqlGatewayHost, sqlGatewayPort);

    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", GRAVITINO_METALAKE);
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);
    configuration.setString("sql-gateway.endpoint.rest.address", sqlGatewayHost);
    configuration.setInteger("sql-gateway.endpoint.rest.port", sqlGatewayPort);
    EnvironmentSettings.Builder builder =
        EnvironmentSettings.newInstance().withConfiguration(configuration);
    tableEnv = TableEnvironment.create(builder.inBatchMode().build());
    DefaultContext defaultContext = new DefaultContext(configuration, Collections.emptyList());
    sqlGateway =
        new SqlGateway(defaultContext.getFlinkConfig(), SessionManager.create(defaultContext));
    sqlGateway.start();
  }

  private static void stopFlinkEnv() {
    if (tableEnv != null) {
      try {
        TableEnvironmentImpl env = (TableEnvironmentImpl) tableEnv;
        env.getCatalogManager().close();
        sqlGateway.stop();
      } catch (Exception e) {
        LOG.error("Close Flink environment failed", e);
      }
    }
  }

  private HttpResponse<String> sendOpenSessionRequest() throws IOException, InterruptedException {
    String urlString = String.format("%s/v1/sessions", sqlGatewayRestUri);
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(urlString))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  @Test
  public void testCreateGravitinoCatalogManager() throws IOException, InterruptedException {
    Assertions.assertEquals(200, this.sendOpenSessionRequest().statusCode());
    Assertions.assertEquals(200, this.sendOpenSessionRequest().statusCode());
  }
}
