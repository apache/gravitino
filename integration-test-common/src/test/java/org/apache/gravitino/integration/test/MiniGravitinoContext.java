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

package org.apache.gravitino.integration.test;

import java.util.Map;
import org.apache.gravitino.server.GravitinoServer;

public class MiniGravitinoContext {

  /**
   * Starts a Gravitino server from a configuration file, in the current JVM.
   *
   * <p>A distribution that ships its own entry point initializes components the open source server
   * knows nothing about. Without a way to name that entry point, its integration tests can only run
   * in deploy mode, and in embedded mode those components are quietly absent: the event listeners
   * that depend on them fail on every event, the framework logs and swallows the failures, and
   * tests that tolerate empty results pass while exercising nothing.
   */
  @FunctionalInterface
  public interface ServerLauncher {

    /**
     * Starts the server.
     *
     * @param args The arguments to start the server with, the configuration file path.
     * @throws Exception If the server fails to start.
     */
    void launch(String[] args) throws Exception;
  }

  /** Starts the open source {@link GravitinoServer}, which is what most callers want. */
  public static final ServerLauncher DEFAULT_SERVER_LAUNCHER = GravitinoServer::main;

  Map<String, String> customConfig;
  final boolean ignoreIcebergAuxRestService;
  final boolean ignoreLanceAuxRestService;
  private final ServerLauncher serverLauncher;

  public MiniGravitinoContext(
      Map<String, String> customConfig,
      boolean ignoreIcebergAuxRestService,
      boolean ignoreLanceAuxRestService) {
    this(
        customConfig,
        ignoreIcebergAuxRestService,
        ignoreLanceAuxRestService,
        DEFAULT_SERVER_LAUNCHER);
  }

  /**
   * Creates a context that starts the given server rather than the open source one.
   *
   * @param customConfig The configuration entries to write into the server configuration.
   * @param ignoreIcebergAuxRestService Whether to leave the Iceberg REST service out.
   * @param ignoreLanceAuxRestService Whether to leave the Lance REST service out.
   * @param serverLauncher The server to start.
   */
  public MiniGravitinoContext(
      Map<String, String> customConfig,
      boolean ignoreIcebergAuxRestService,
      boolean ignoreLanceAuxRestService,
      ServerLauncher serverLauncher) {
    this.customConfig = customConfig;
    this.ignoreIcebergAuxRestService = ignoreIcebergAuxRestService;
    this.ignoreLanceAuxRestService = ignoreLanceAuxRestService;
    this.serverLauncher = serverLauncher;
  }

  /**
   * Returns the server this context starts.
   *
   * @return the server launcher.
   */
  public ServerLauncher serverLauncher() {
    return serverLauncher;
  }
}
