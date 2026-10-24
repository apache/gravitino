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

package org.apache.gravitino.spark.connector.plugin;

import java.io.IOException;
import java.io.InputStream;
import org.apache.spark.api.plugin.SparkPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests that this module's bindings are complete. Constructing the driver plugin runs {@code
 * SparkBindings.build()}, whose Preconditions reject a missing binding, so a catalog this module
 * forgot to bind fails here rather than at session startup.
 */
public class TestGravitinoSparkPlugin {

  @Test
  void testTheBindingsThisModuleDeclaresAreComplete() {
    Assertions.assertNotNull(new GravitinoSparkPlugin().driverPlugin());
  }

  @Test
  void testDriverPluginDoesNotRequireOptionalIcebergRuntime() throws Exception {
    ClassLoader loader = new WithoutIcebergClassLoader(GravitinoSparkPlugin.class.getClassLoader());
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      Class<?> pluginClass = Class.forName(GravitinoSparkPlugin.class.getName(), true, loader);
      SparkPlugin plugin = (SparkPlugin) pluginClass.getConstructor().newInstance();

      Assertions.assertNotNull(plugin.driverPlugin());
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }
  }

  private static class WithoutIcebergClassLoader extends ClassLoader {

    private WithoutIcebergClassLoader(ClassLoader parent) {
      super(parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.startsWith("org.apache.iceberg.")
          || name.startsWith("org.apache.spark.sql.connector.iceberg.")) {
        throw new ClassNotFoundException(name);
      }
      if (!name.startsWith("org.apache.gravitino.spark.connector.")) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          try (InputStream input =
              getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
            if (input == null) {
              throw new ClassNotFoundException(name);
            }
            byte[] bytes = input.readAllBytes();
            loaded = defineClass(name, bytes, 0, bytes.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }
}
