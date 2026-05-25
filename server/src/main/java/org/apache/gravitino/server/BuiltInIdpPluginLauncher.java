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

package org.apache.gravitino.server;

import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the built-in IdP plugin from the runtime classpath without a compile-time module
 * dependency.
 */
final class BuiltInIdpPluginLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(BuiltInIdpPluginLauncher.class);

  private static final String IDP_STORAGE_BOOTSTRAP_CLASS =
      "org.apache.gravitino.idp.storage.IdpStorageBootstrap";

  private BuiltInIdpPluginLauncher() {}

  /**
   * Initializes built-in IdP storage when the idp-basic plugin jar is present on the classpath.
   *
   * <p>This is a no-op when the plugin is absent. Startup failures from the plugin are propagated
   * to fail server initialization.
   */
  static void initializeOnceIfPresent() {
    try {
      Class<?> bootstrapClass = Class.forName(IDP_STORAGE_BOOTSTRAP_CLASS);
      bootstrapClass.getMethod("initializeOnce").invoke(null);
    } catch (ClassNotFoundException e) {
      LOG.debug("Built-in IdP plugin is not on the classpath, skipping IdP bootstrap");
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new IllegalStateException("Failed to initialize built-in IdP plugin", cause);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize built-in IdP plugin", e);
    }
  }
}
