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

package org.apache.gravitino.hive.client;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.spi.LoggerContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

public class TestHiveClientClassLoader {

  @Test
  void testCloseRemovesLog4jLoggerContextRegistration() throws Exception {
    HiveClientClassLoader classLoader =
        HiveClientClassLoader.createLoader(
            HiveClientClassLoader.HiveVersion.HIVE3, getClass().getClassLoader());

    // Trigger initialization of Util, a barrier class that is defined directly inside this
    // classloader (see HiveClientClassLoader#loadBarrierClass), just like HiveClientImpl and
    // HiveShim are in production. Util's static logger initialization
    // (LoggerFactory.getLogger(Util.class)) makes log4j-slf4j2-impl's Log4jLoggerFactory walk the
    // call stack to Util as the anchor class and register a LoggerContext keyed by Util's
    // defining classloader, i.e. this HiveClientClassLoader, in its registry map. That map is
    // keyed by the LoggerContext object itself (a strong reference), so the LoggerContext - and
    // through it this classloader - stays reachable until the context is explicitly shut down.
    Class<?> utilClass = classLoader.loadClass(Util.class.getName());
    Assertions.assertSame(
        classLoader, utilClass.getClassLoader(), "Util should be defined by the isolated loader");
    Method updateConfig =
        utilClass.getMethod(
            "updateConfigurationFromProperties", Properties.class, Configuration.class);
    updateConfig.invoke(null, new Properties(), new Configuration());

    LoggerContext context = LogManager.getContext(classLoader, false);
    Map<LoggerContext, ?> registry = getSlf4jAdapterRegistry();
    Assertions.assertTrue(
        registry.containsKey(context),
        "Precondition failed: Log4jLoggerFactory should have registered a LoggerContext for "
            + "the isolated classloader");

    classLoader.close();

    Assertions.assertFalse(
        registry.containsKey(context),
        "HiveClientClassLoader#close() should shut down its Log4j LoggerContext so that "
            + "Log4jLoggerFactory's registry releases its strong reference to it (and "
            + "transitively to the classloader); otherwise the classloader can never be "
            + "garbage collected");
  }

  /** Reflectively reads the {@code registry} field of {@code AbstractLoggerAdapter}. */
  @SuppressWarnings("unchecked")
  private static Map<LoggerContext, ?> getSlf4jAdapterRegistry() throws Exception {
    ILoggerFactory adapter = LoggerFactory.getILoggerFactory();
    Field registryField = adapter.getClass().getSuperclass().getDeclaredField("registry");
    registryField.setAccessible(true);
    return (Map<LoggerContext, ?>) registryField.get(adapter);
  }
}
