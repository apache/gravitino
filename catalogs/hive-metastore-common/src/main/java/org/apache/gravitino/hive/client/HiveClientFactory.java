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

import static org.apache.gravitino.catalog.hive.HiveConstants.HIVE_METASTORE_URIS;
import static org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion.HIVE2;
import static org.apache.gravitino.hive.client.HiveClientClassLoader.HiveVersion.HIVE3;
import static org.apache.gravitino.hive.client.Util.buildConfigurationFromProperties;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HiveClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveClientFactory.class);

  // Remember which Hive backend classloader worked successfully for this factory.
  private volatile HiveClientClassLoader backendClassLoader;
  private final Object classLoaderLock = new Object();

  @SuppressWarnings("UnusedVariable")
  private final Configuration hadoopConf;

  private final Properties properties;

  /**
   * Creates a {@link HiveClientFactory} bound to the given configuration properties.
   *
   * @param properties Hive client configuration, must not be null.
   * @param id An identifier for this factory instance.
   */
  public HiveClientFactory(Properties properties, String id) {
    Preconditions.checkArgument(properties != null, "Properties cannot be null");
    this.properties = properties;

    try {
      this.hadoopConf = buildConfigurationFromProperties(properties);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize HiveClientFactory", e);
    }
  }

  public HiveClient createHiveClient() {
    HiveClientClassLoader classLoader;
    if (backendClassLoader == null) {
      synchronized (classLoaderLock) {
        if (backendClassLoader == null) {
          // initialize the backend classloader with try connecting to Hive metastore
          return createHiveClientWithBackend();
        }
      }
    }
    classLoader = backendClassLoader;

    HiveClient client;
    try {
      client = createHiveClientInternal(classLoader);
      LOG.info(
          "Connected to Hive Metastore using cached Hive version {}", classLoader.getHiveVersion());
      return client;
    } catch (Exception e) {
      LOG.warn(
          "Failed to connect to Hive Metastore using cached Hive version {}",
          classLoader.getHiveVersion(),
          e);
      throw new RuntimeException("Failed to connect to Hive Metastore", e);
    }
  }

  public HiveClient createHiveClientWithBackend() {
    HiveClient client = null;
    HiveClientClassLoader classloader = null;
    try {
      // Try using Hive3 first
      classloader =
          HiveClientClassLoader.createLoader(HIVE3, Thread.currentThread().getContextClassLoader());
      client = createHiveClientInternal(classloader);
      client.getCatalogs();
      LOG.info("Connected to Hive Metastore using Hive version HIVE3");
      backendClassLoader = classloader;
      return client;

    } catch (GravitinoRuntimeException e) {
      try {
        if (client != null) {
          client.close();
        }
        if (classloader != null) {
          classloader.close();
        }

        // Fallback to Hive2 if we can list databases
        if (e.getMessage().contains("Invalid method name: 'get_catalogs'")
            || e.getMessage().contains("class not found") // caused by MiniHiveMetastoreService
        ) {
          classloader =
              HiveClientClassLoader.createLoader(
                  HIVE2, Thread.currentThread().getContextClassLoader());
          client = createHiveClientInternal(classloader);
          LOG.info("Connected to Hive Metastore using Hive version HIVE2");
          backendClassLoader = classloader;
          return client;
        }
        throw e;

      } catch (Exception ex) {
        LOG.error("Failed to connect to Hive Metastore using both Hive3 and Hive2", ex);
        throw e;
      }
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e, HiveExceptionConverter.ExceptionTarget.other(""));
    }
  }

  public static HiveClient createHiveClientImpl(
      HiveClientClassLoader.HiveVersion version, Properties properties, ClassLoader classloader)
      throws Exception {
    Class<?> hiveClientImplClass = classloader.loadClass(HiveClientImpl.class.getName());
    Constructor<?> hiveClientImplCtor =
        hiveClientImplClass.getConstructor(
            HiveClientClassLoader.HiveVersion.class, Properties.class);
    return (HiveClient) hiveClientImplCtor.newInstance(version, properties);
  }

  public static HiveClient createProxyHiveClientImpl(
      HiveClientClassLoader.HiveVersion version,
      Properties properties,
      UserGroupInformation ugi,
      ClassLoader classloader)
      throws Exception {
    Class<?> hiveClientImplClass = classloader.loadClass(ProxyHiveClientImpl.class.getName());
    Method createMethod =
        MethodUtils.getAccessibleMethod(
            hiveClientImplClass,
            "createClient",
            HiveClientClassLoader.HiveVersion.class,
            UserGroupInformation.class,
            Properties.class);
    return (HiveClient) createMethod.invoke(null, version, ugi, properties);
  }

  private HiveClient createHiveClientInternal(HiveClientClassLoader classloader) {
    ClassLoader origLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classloader);
    try {
      UserGroupInformation ugi;
      ugi = UserGroupInformation.getCurrentUser();
      if (!ugi.getUserName().equals(PrincipalUtils.getCurrentUserName())) {
        ugi = UserGroupInformation.createProxyUser(PrincipalUtils.getCurrentUserName(), ugi);
      }
      return createProxyHiveClientImpl(classloader.getHiveVersion(), properties, ugi, classloader);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e,
          HiveExceptionConverter.ExceptionTarget.other(
              properties.getProperty(HIVE_METASTORE_URIS)));
    } finally {
      Thread.currentThread().setContextClassLoader(origLoader);
    }
  }

  /** Release resources held by this factory. */
  public void close() {
    synchronized (classLoaderLock) {
      try {
        if (backendClassLoader != null) {
          backendClassLoader.close();
          backendClassLoader = null;
        }

      } catch (Exception e) {
        LOG.warn("Failed to close HiveClientFactory", e);
      }
    }
  }
}
