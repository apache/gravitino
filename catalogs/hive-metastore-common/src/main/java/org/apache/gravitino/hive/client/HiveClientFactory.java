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

import static org.apache.gravitino.hive.client.HiveClientClassLoader.getClientLoaderForVersion;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.hive.kerberos.AuthenticationConfig;
import org.apache.gravitino.hive.kerberos.KerberosClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HiveClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveClientFactory.class);

  public static final String HIVE_CONFIG_RESOURCES = "hive.config.resources";

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-hive-%s-keytab";
  private static final String RETRYING_META_STORE_CLIENT_CLASS =
      "org.apache.hadoop.hive.metastore.RetryingMetaStoreClient";
  private static final String HIVE_CONF_CLASS = "org.apache.hadoop.hive.conf.HiveConf";
  private static final String CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";
  private static final String METHOD_GET_PROXY = "getProxy";

  // Singleton instance
  private static final HiveClientFactory INSTANCE = new HiveClientFactory();

  private HiveClientFactory() {}

  public static HiveClientFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Public entry point to create a {@link HiveClient}. Keeps a static signature for convenience,
   * while delegating to the singleton instance for actual work.
   */
  public static HiveClient createHiveClient(Properties properties) {
    return INSTANCE.createHiveClientInternal(properties);
  }

  private HiveClient createHiveClientInternal(Properties properties) {
    Preconditions.checkArgument(properties != null, "Properties cannot be null");

    HiveClient client = null;
    try {
      // Try using Hive3 first
      HiveClientClassLoader classloader =
          getClientLoaderForVersion(HiveClientClassLoader.HiveVersion.HIVE2, properties);
      client = createHiveClientInternal(properties, classloader);
      client.getCatalogs();
      LOG.info("Connected to Hive Metastore using Hive version HIVE3");
      return client;

    } catch (GravitinoRuntimeException e) {
      if (client != null) {
        client.close();
      }

      try {
        // Fallback to Hive2 if we can list databases

        if (e.getMessage().contains("Invalid method name: 'get_catalogs'")
            || e.getMessage().contains("class not found") // caused by MiniHiveMetastoreService
        ) {
          HiveClientClassLoader classloader =
              getClientLoaderForVersion(HiveClientClassLoader.HiveVersion.HIVE2, properties);
          client = createHiveClientInternal(properties, classloader);
          LOG.info("Connected to Hive Metastore using Hive version HIVE2");
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

  /**
   * Wraps the given {@link HiveClient} with Kerberos-aware proxy when authentication.type is set to
   * "kerberos". Otherwise returns the original client.
   */
  private HiveClient wrapWithKerberosIfNeeded(Properties properties, HiveClient client) {
    String authType = properties.getProperty("authentication.type");
    if (authType != null && "kerberos".equalsIgnoreCase(authType)) {
      return KerberosHiveClientImpl.createClient(client);
    }
    return client;
  }

  private HiveClient createHiveClientInternal(
      Properties properties, HiveClientClassLoader classloader) throws Exception {
    ClassLoader origLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classloader);
    try {
      return (HiveClient) createHiveClientImpl(properties, classloader);
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e,
          HiveExceptionConverter.ExceptionTarget.other(
              properties.getProperty("hive.metastore.uris")));
    } finally {
      Thread.currentThread().setContextClassLoader(origLoader);
    }
  }

  /**
   * Creates a Hive Metastore client instance based on the version.
   *
   * @param properties Hive configuration properties
   * @return The Metastore client object
   * @throws Exception If client creation fails
   */
  private Object createHiveClientImpl(Properties properties, HiveClientClassLoader classLoader)
      throws Exception {
    Class<?> clientClass = classLoader.loadClass(RETRYING_META_STORE_CLIENT_CLASS);
    Class<?> hiveConfClass = classLoader.loadClass(HIVE_CONF_CLASS);
    Class<?> confClass = classLoader.loadClass(CONFIGURATION_CLASS);

    Object hiveClient;
    HiveClientClassLoader.HiveVersion version = classLoader.getHiveVersion();
    if (version == HiveClientClassLoader.HiveVersion.HIVE2) {
      hiveClient = createHive2Client(clientClass, hiveConfClass, confClass, properties);
    } else if (version == HiveClientClassLoader.HiveVersion.HIVE3) {
      hiveClient = createHive3Client(clientClass, confClass, properties);
    } else {
      throw new IllegalArgumentException("Unsupported Hive version: " + version);
    }

    Class<?> clientImplClass = classLoader.loadClass(HiveClientImpl.class.getName());
    Constructor<?> constructor = clientImplClass.getConstructors()[0];
    HiveClient client = (HiveClient) constructor.newInstance(version, hiveClient);
    return wrapWithKerberosIfNeeded(properties, client);
  }

  /**
   * Creates a Hive 2.x Metastore client.
   *
   * @param clientClass The RetryingMetaStoreClient class
   * @param hiveConfClass The HiveConf class
   * @param confClass The Configuration class
   * @param properties Configuration properties
   * @return The Metastore client instance
   * @throws Exception If client creation fails
   */
  private Object createHive2Client(
      Class<?> clientClass, Class<?> hiveConfClass, Class<?> confClass, Properties properties)
      throws Exception {
    Object conf = confClass.getDeclaredConstructor().newInstance();
    buildConfiguration((Map) properties, (Configuration) conf);
    initKerberosIfNecessary(properties, (Configuration) conf);

    Constructor<?> hiveConfCtor = hiveConfClass.getConstructor(confClass, Class.class);
    Object hiveConfInstance = hiveConfCtor.newInstance(conf, hiveConfClass);

    Method getProxyMethod = clientClass.getMethod(METHOD_GET_PROXY, hiveConfClass, boolean.class);
    return getProxyMethod.invoke(null, hiveConfInstance, false);
  }

  /**
   * Creates a Hive 3.x Metastore client.
   *
   * @param clientClass The RetryingMetaStoreClient class
   * @param confClass The Configuration class
   * @param properties Configuration properties
   * @return The Metastore client instance
   * @throws Exception If client creation fails
   */
  private Object createHive3Client(Class<?> clientClass, Class<?> confClass, Properties properties)
      throws Exception {
    Object conf = confClass.getDeclaredConstructor().newInstance();
    buildConfiguration((Map) properties, (Configuration) conf);
    initKerberosIfNecessary(properties, (Configuration) conf);

    Method getProxyMethod = clientClass.getMethod(METHOD_GET_PROXY, confClass, boolean.class);
    return getProxyMethod.invoke(null, conf, true);
  }

  private boolean initKerberosIfNecessary(Properties conf, Configuration hadoopConf) {
    Map<String, String> properties = (Map) conf;
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties, hadoopConf);
    if (authenticationConfig.isKerberosAuth()) {
      try (KerberosClient kerberosClient = new KerberosClient(properties, hadoopConf, true)) {
        String keytabPath =
            String.format(GRAVITINO_KEYTAB_FORMAT, properties.getOrDefault("id", ""));
        File keytabFile = kerberosClient.saveKeyTabFileFromUri(keytabPath);
        kerberosClient.login(keytabFile.getAbsolutePath());
        LOG.info("Login with kerberos success");
        return true;
      } catch (IOException e) {
        throw new RuntimeException("Failed to login with kerberos", e);
      }
    }
    return false;
  }

  public void buildConfiguration(Map<String, String> config, Configuration configuration) {
    try {
      String hdfsConfigResources = config.get(HIVE_CONFIG_RESOURCES);
      if (StringUtils.isNotBlank(hdfsConfigResources)) {
        for (String resource : hdfsConfigResources.split(",")) {
          resource = resource.trim();
          if (StringUtils.isNotBlank(resource)) {
            configuration.addResource(new Path(resource));
          }
        }
      }

      config.forEach(configuration::set);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create configuration", e);
    }
  }
}
