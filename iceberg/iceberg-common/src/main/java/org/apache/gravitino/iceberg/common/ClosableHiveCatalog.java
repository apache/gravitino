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

package org.apache.gravitino.iceberg.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import org.apache.gravitino.iceberg.common.utils.CaffeineSchedulerExtractorUtils;
import org.apache.gravitino.iceberg.common.utils.IcebergHiveCachedClientPool;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClosableHiveCatalog is a wrapper class to wrap Iceberg HiveCatalog to do some clean-up work like
 * closing resources.
 */
public class ClosableHiveCatalog extends HiveCatalog implements Closeable, SupportsKerberos {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosableHiveCatalog.class);

  @Getter private final List<Closeable> resources = Lists.newArrayList();

  private KerberosClient kerberosClient;

  public ClosableHiveCatalog() {
    super();
  }

  public void addResource(Closeable resource) {
    resources.add(resource);
  }

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    super.initialize(inputName, properties);

    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
    if (authenticationConfig.isKerberosAuth()) {
      this.kerberosClient = initKerberosClient();
    }

    try {
      resetIcebergHiveClientPool();
    } catch (Exception e) {
      throw new RuntimeException("Failed to reset IcebergHiveClientPool", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (kerberosClient != null) {
      try {
        kerberosClient.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close KerberosClient", e);
      }
    }

    // Do clean up work here. We need a mechanism to close the HiveCatalog; however, HiveCatalog
    // doesn't implement the Closeable interface.

    // First, close the internal HiveCatalog client pool to prevent resource leaks
    closeInternalClientPool();

    // Then close any additional resources added via addResource()
    resources.forEach(
        resource -> {
          try {
            if (resource != null) {
              resource.close();
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to close resource: {}", resource, e);
          }
        });
  }

  @Override
  public <R> R doKerberosOperations(Map<String, String> properties, Executable<R> executable)
      throws Throwable {

    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
    if (!authenticationConfig.isKerberosAuth()) {
      return executable.execute();
    }

    final String finalPrincipalName;
    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();

    if (!proxyKerberosPrincipalName.contains("@")) {
      finalPrincipalName =
          String.format("%s@%s", proxyKerberosPrincipalName, kerberosClient.getRealm());
    } else {
      finalPrincipalName = proxyKerberosPrincipalName;
    }

    UserGroupInformation realUser =
        authenticationConfig.isImpersonationEnabled()
            ? UserGroupInformation.createProxyUser(
                finalPrincipalName, kerberosClient.getLoginUser())
            : kerberosClient.getLoginUser();
    try {
      ClientPool<IMetaStoreClient, TException> newClientPool =
          (ClientPool<IMetaStoreClient, TException>) FieldUtils.readField(this, "clients", true);
      kerberosClient
          .getLoginUser()
          .doAs(
              (PrivilegedExceptionAction<Void>)
                  () -> {
                    String token =
                        newClientPool.run(
                            client ->
                                client.getDelegationToken(
                                    finalPrincipalName,
                                    kerberosClient.getLoginUser().getShortUserName()));

                    Token<DelegationTokenIdentifier> delegationToken = new Token<>();
                    delegationToken.decodeFromUrlString(token);
                    realUser.addToken(delegationToken);
                    return null;
                  });
    } catch (Exception e) {
      throw new RuntimeException("Failed to get delegation token", e);
    }
    return (R)
        realUser.doAs(
            (PrivilegedExceptionAction<Object>)
                () -> {
                  try {
                    return executable.execute();
                  } catch (Throwable e) {
                    if (RuntimeException.class.isAssignableFrom(e.getClass())) {
                      throw (RuntimeException) e;
                    }
                    throw new RuntimeException("Failed to invoke method", e);
                  }
                });
  }

  private ClientPool<IMetaStoreClient, TException> resetIcebergHiveClientPool()
      throws IllegalAccessException {
    // Get the old client pool before replacing it
    Object oldPool = FieldUtils.readField(this, "clients", true);

    // Create and set the new client pool first
    IcebergHiveCachedClientPool newClientPool =
        new IcebergHiveCachedClientPool(this.getConf(), this.properties());
    FieldUtils.writeField(this, "clients", newClientPool, true);

    // Then try to close the old pool to release resources
    if (oldPool != null) {
      // Try standard close method if available
      if (oldPool instanceof AutoCloseable) {
        try {
          ((AutoCloseable) oldPool).close();
          LOGGER.info("Successfully closed old Hive client pool");
        } catch (Exception e) {
          LOGGER.warn("Failed to close old Hive client pool", e);
        }
      }

      // Additionally, try to shutdown the internal scheduler thread pool in Iceberg's
      // CachedClientPool to prevent memory leak. This is necessary because Iceberg's
      // CachedClientPool does not implement Closeable and does not properly clean up
      // its internal scheduler.
      try {
        shutdownIcebergCachedClientPoolScheduler(oldPool);
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to shutdown scheduler in old CachedClientPool, may cause minor resource leak",
            e);
      }
    }

    return newClientPool;
  }

  /**
   * Shuts down the scheduler thread pool in Iceberg's CachedClientPool.
   *
   * <p>Required because CachedClientPool doesn't provide cleanup, causing thread pool leaks.
   *
   * @param clientPool The old CachedClientPool instance
   */
  @VisibleForTesting
  void shutdownIcebergCachedClientPoolScheduler(Object clientPool) {
    try {
      Object cache = FieldUtils.readField(clientPool, "clientPoolCache", true);
      if (cache == null) {
        LOGGER.debug("clientPoolCache is null, no scheduler to shutdown");
        return;
      }

      ScheduledExecutorService executor =
          CaffeineSchedulerExtractorUtils.extractSchedulerExecutor(cache);
      if (executor != null) {
        LOGGER.info("Shutting down scheduler thread pool from old CachedClientPool");
        executor.shutdownNow();
      } else {
        LOGGER.debug("Could not extract scheduler executor from cache");
      }
    } catch (IllegalAccessException e) {
      LOGGER.debug("Failed to access clientPoolCache field", e);
    }
  }

  /**
   * Close the internal HiveCatalog client pool using reflection. This is necessary because
   * HiveCatalog doesn't provide a public API to close its client pool. We need to avoid closing
   * IcebergHiveCachedClientPool twice (once here and once in resources list).
   */
  private void closeInternalClientPool() {
    try {
      Field clientsField = HiveCatalog.class.getDeclaredField("clients");
      clientsField.setAccessible(true);
      Object clientPool = clientsField.get(this);

      if (clientPool != null && clientPool instanceof AutoCloseable) {
        // Only close if it's NOT IcebergHiveCachedClientPool
        if (!(clientPool instanceof IcebergHiveCachedClientPool)) {
          ((AutoCloseable) clientPool).close();
          LOGGER.info(
              "Closed HiveCatalog internal client pool: {}", clientPool.getClass().getSimpleName());
        }
      }
    } catch (NoSuchFieldException e) {
      LOGGER.warn("Could not find 'clients' field in HiveCatalog, skipping cleanup", e);
    } catch (Exception e) {
      LOGGER.warn("Failed to close HiveCatalog internal client pool", e);
    }
  }

  private KerberosClient initKerberosClient() {
    try {
      KerberosClient kerberosClient = new KerberosClient(this.properties(), this.getConf());
      // For Iceberg rest server, we haven't set the catalog_uuid, so we set it to 0 as there is
      // only one catalog in the rest server, so it's okay to set it to 0.
      String catalogUUID = properties().getOrDefault("catalog_uuid", "0");
      File keytabFile = kerberosClient.saveKeyTabFileFromUri(Long.valueOf(catalogUUID));
      kerberosClient.login(keytabFile.getAbsolutePath());
      return kerberosClient;
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with kerberos", e);
    }
  }
}
