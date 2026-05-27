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

package org.apache.gravitino.server.plugin;

import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads and runs {@link ServerPluginBootstrap} providers from the server classpath. */
public final class ServerPluginBootstrapper {

  private static final Logger LOG = LoggerFactory.getLogger(ServerPluginBootstrapper.class);

  private ServerPluginBootstrapper() {}

  /** Initializes all {@link ServerPluginBootstrap} providers present on the classpath. */
  public static void initialize() {
    ServiceLoader<ServerPluginBootstrap> loader = ServiceLoader.load(ServerPluginBootstrap.class);
    for (ServerPluginBootstrap bootstrap : loader) {
      try {
        LOG.info("Initializing server plugin bootstrap: {}", bootstrap.name());
        bootstrap.initialize();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Failed to initialize server plugin bootstrap: %s", bootstrap.name()), e);
      }
    }
  }

  /** Stops all {@link ServerPluginBootstrap} providers present on the classpath. */
  public static void stop() {
    ServiceLoader<ServerPluginBootstrap> loader = ServiceLoader.load(ServerPluginBootstrap.class);
    for (ServerPluginBootstrap bootstrap : loader) {
      try {
        LOG.info("Stopping server plugin bootstrap: {}", bootstrap.name());
        bootstrap.stop();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IllegalStateException(
            String.format("Failed to stop server plugin bootstrap: %s", bootstrap.name()), e);
      }
    }
  }
}
