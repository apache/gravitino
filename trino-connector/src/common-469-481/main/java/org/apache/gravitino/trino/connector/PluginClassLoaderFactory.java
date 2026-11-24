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
package org.apache.gravitino.trino.connector;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.List;

/**
 * Instantiates Trino's internal {@code io.trino.server.PluginClassLoader}. The class is not part of
 * the Trino SPI, so {@link GravitinoConnectorPluginManager} loads it by name through the app class
 * loader and this factory reflects on it to cross that isolation boundary.
 *
 * <p>Trino 440-481 expose {@code PluginClassLoader(String, List<URL>, ClassLoader, List<String>
 * spiPackages)}.
 */
final class PluginClassLoaderFactory {

  private PluginClassLoaderFactory() {}

  static Object create(
      Class<?> pluginLoaderClass,
      String classLoaderName,
      List<URL> urls,
      ClassLoader parent,
      List<String> spiPackages)
      throws Exception {
    Constructor<?> constructor =
        pluginLoaderClass.getConstructor(String.class, List.class, ClassLoader.class, List.class);
    return constructor.newInstance(classLoaderName, urls, parent, spiPackages);
  }
}
