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
import static org.apache.gravitino.hive.client.Util.buildConfiguration;

import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.hive.kerberos.AuthenticationConfig;
import org.apache.gravitino.hive.kerberos.KerberosClient;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HiveClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveClientFactory.class);

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-hive-%s-keytab";

  // Singleton instance
  private static final HiveClientFactory INSTANCE = new HiveClientFactory();

  private KerberosClient kerberosClient;

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
          getClientLoaderForVersion(HiveClientClassLoader.HiveVersion.HIVE3, properties);
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

  public static HiveClient createHiveClientImpl(
      HiveClientClassLoader.HiveVersion version, Properties properties, ClassLoader classloader)
      throws Exception {
    Class<?> hiveClientImplClass = classloader.loadClass(HiveClientImpl.class.getName());
    Constructor<?> hiveClientImplCtor =
        hiveClientImplClass.getConstructor(
            HiveClientClassLoader.HiveVersion.class, Properties.class);
    return (HiveClient) hiveClientImplCtor.newInstance(version, properties);
  }

  private HiveClient createHiveClientInternal(
      Properties properties, HiveClientClassLoader classloader) {
    ClassLoader origLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classloader);
    try {
      UserGroupInformation ugi = initKerberosIfNecessary(classloader.getHiveVersion(), properties);
      if (ugi == null) {
        return createHiveClientImpl(classloader.getHiveVersion(), properties, classloader);
      } else {
        Class<?> hiveClientImplClass =
            classloader.loadClass(KerberosHiveClientImpl.class.getName());
        Method createMethod =
            Util.findStaticMethod(
                hiveClientImplClass,
                "createClient",
                HiveClientClassLoader.HiveVersion.class,
                UserGroupInformation.class,
                Properties.class);
        return (HiveClient)
            createMethod.invoke(null, classloader.getHiveVersion(), ugi, properties);
      }
    } catch (Exception e) {
      throw HiveExceptionConverter.toGravitinoException(
          e,
          HiveExceptionConverter.ExceptionTarget.other(
              properties.getProperty("hive.metastore.uris")));
    } finally {
      Thread.currentThread().setContextClassLoader(origLoader);
    }
  }

  private UserGroupInformation initKerberosIfNecessary(
      HiveClientClassLoader.HiveVersion version, Properties conf) {
    Configuration hadoopConf = new Configuration();
    buildConfiguration(conf, hadoopConf);
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(conf, hadoopConf);

    if (!authenticationConfig.isKerberosAuth()) {
      return null;
    }

    try {
      if (kerberosClient != null) {
        return kerberosClient.login(PrincipalUtils.getCurrentUserName());
      }

      String keytabPath = String.format(GRAVITINO_KEYTAB_FORMAT, conf.getOrDefault("id", ""));
      kerberosClient = new KerberosClient(version, conf, hadoopConf, true, keytabPath);
      kerberosClient.saveKeyTabFileFromUri();
      return kerberosClient.login(PrincipalUtils.getCurrentUserName());

    } catch (Exception e) {
      throw new RuntimeException("Failed to login with kerberos", e);
    }
  }
}
