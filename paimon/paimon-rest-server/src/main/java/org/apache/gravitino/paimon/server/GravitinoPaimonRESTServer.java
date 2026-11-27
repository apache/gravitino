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
package org.apache.gravitino.paimon.server;

import com.google.common.base.Preconditions;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.paimon.PaimonConfig;
import org.apache.gravitino.paimon.RESTService;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authentication.ServerAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone entry point of the Paimon REST catalog server. Loads {@code
 * gravitino-paimon-rest-server.conf} and runs {@link RESTService} outside of a Gravitino server
 * process. The service currently supports authentication and requires Gravitino role-based access
 * control to be disabled.
 */
public class GravitinoPaimonRESTServer {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoPaimonRESTServer.class);

  /** The default standalone configuration file. */
  public static final String CONF_FILE = "gravitino-paimon-rest-server.conf";

  private final ServerConfig serverConfig;

  private final RESTService paimonRESTService;
  private final GravitinoEnv gravitinoEnv;

  /**
   * Creates a standalone server.
   *
   * @param config the server configuration
   */
  public GravitinoPaimonRESTServer(ServerConfig config) {
    this.serverConfig = config;
    this.gravitinoEnv = GravitinoEnv.getInstance();
    this.paimonRESTService = new RESTService();
  }

  private void initialize() {
    gravitinoEnv.initializeBaseComponents(serverConfig);
    ServerAuthenticator.getInstance().initialize(serverConfig);
    paimonRESTService.serviceInit(
        serverConfig.getConfigsWithPrefix(PaimonConfig.PAIMON_REST_CONFIG_PREFIX), false);
  }

  private void start() {
    gravitinoEnv.start();
    paimonRESTService.serviceStart();
  }

  private void join() {
    paimonRESTService.join();
  }

  private void stop() {
    try {
      paimonRESTService.serviceStop();
    } catch (Exception e) {
      LOG.error("Error while stopping Gravitino Paimon REST server", e);
    } finally {
      gravitinoEnv.shutdown();
    }
  }

  /**
   * Main entry point of the standalone Paimon REST server.
   *
   * @param args an optional path to a standalone configuration file
   */
  public static void main(String[] args) {
    LOG.info("Starting Gravitino Paimon REST Server");
    String confPath = args.length == 0 ? "" : args[0];
    ServerConfig serverConfig = ServerConfig.loadConfig(confPath, CONF_FILE);
    Preconditions.checkArgument(
        !serverConfig.get(Configs.ENABLE_AUTHORIZATION),
        "Paimon REST server standalone mode doesn't support authorization.");
    GravitinoPaimonRESTServer paimonRESTServer = new GravitinoPaimonRESTServer(serverConfig);
    try {
      paimonRESTServer.initialize();
      paimonRESTServer.start();
    } catch (Exception e) {
      LOG.error("Error while starting Gravitino Paimon REST server", e);
      paimonRESTServer.stop();
      System.exit(-1);
    }
    LOG.info("Done, Gravitino Paimon REST server started.");

    Runtime.getRuntime()
        .addShutdownHook(new Thread(paimonRESTServer::stop, "paimon-rest-shutdown"));
    paimonRESTServer.join();
  }
}
