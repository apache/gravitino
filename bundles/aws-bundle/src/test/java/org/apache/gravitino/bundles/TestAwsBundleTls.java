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
package org.apache.gravitino.bundles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class TestAwsBundleTls {
  private static final File BUNDLE = new File(System.getProperty("shadowJarPath"));

  @Test
  void testBundleExcludesWildFlyImplementation() throws Exception {
    try (JarFile jar = new JarFile(BUNDLE)) {
      assertFalse(
          jar.stream().anyMatch(entry -> entry.getName().contains("org/wildfly/openssl/")),
          BUNDLE.getName());
    }
  }

  @Test
  void testDefaultJsseModeWorksWithoutWildFly() throws Exception {
    URL[] urls = {
      BUNDLE.toURI().toURL(),
      LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation()
    };
    // The ordinary test classpath also contains unshaded Hadoop and the optional provider.
    try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
      Class<?> factory =
          loader.loadClass("org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory");
      Class<?> mode = loader.loadClass(factory.getName() + "$SSLChannelMode");
      factory
          .getMethod("initializeDefaultFactory", mode)
          .invoke(null, mode.getField("Default_JSSE").get(null));
      Object instance = factory.getMethod("getDefaultFactory").invoke(null);
      assertEquals("Default_JSSE", factory.getMethod("getChannelMode").invoke(instance).toString());
    }
  }
}
