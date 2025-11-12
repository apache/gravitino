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

package org.apache.gravitino.iceberg.common.authentication.kerberos;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.commons.lang3.reflect.FieldUtils;
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
 * Proxy class for HiveCatalog to support kerberos authentication. We can also make HiveCatalog as a
 * generic type and pass it as a parameter to the constructor.
 */
public class HiveBackendProxy implements MethodInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBackendProxy.class);
  private final HiveCatalog target;
  private final String kerberosRealm;
  private final UserGroupInformation proxyUser;
  private final Map<String, String> properties;
  private final ClientPool<IMetaStoreClient, TException> newClientPool;

  public HiveBackendProxy(
      Map<String, String> properties, HiveCatalog target, String kerberosRealm) {
    this.target = target;
    this.properties = properties;
    this.kerberosRealm = kerberosRealm;
    try {
      proxyUser = UserGroupInformation.getCurrentUser();

      // Replace the original client pool with IcebergHiveCachedClientPool. Why do we need to do
      // this? Because the original client pool in Iceberg uses a fixed username to create the
      // client pool, and it will not work with kerberos authentication. We need to create a new
      // client pool with the current user. For more, please see CachedClientPool#clientPool and
      // notice the value of `key`
      this.newClientPool = resetIcebergHiveClientPool();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to reset IcebergHiveClientPool", e);
    }
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    final String finalPrincipalName;
    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();
    if (!proxyKerberosPrincipalName.contains("@")) {
      finalPrincipalName = String.format("%s@%s", proxyKerberosPrincipalName, kerberosRealm);
    } else {
      finalPrincipalName = proxyKerberosPrincipalName;
    }
    UserGroupInformation realUser =
        UserGroupInformation.createProxyUser(finalPrincipalName, proxyUser);

    String token =
        newClientPool.run(
            client -> client.getDelegationToken(finalPrincipalName, proxyUser.getShortUserName()));

    Token<DelegationTokenIdentifier> delegationToken = new Token<>();
    delegationToken.decodeFromUrlString(token);
    realUser.addToken(delegationToken);

    return realUser.doAs(
        (PrivilegedExceptionAction<Object>)
            () -> {
              try {
                return methodProxy.invoke(target, objects);
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
    Object oldPool = FieldUtils.readField(target, "clients", true);

    // Create and set the new client pool first
    IcebergHiveCachedClientPool newClientPool =
        new IcebergHiveCachedClientPool(target.getConf(), properties);
    FieldUtils.writeField(target, "clients", newClientPool, true);

    // Then try to close the old pool to release resources
    if (oldPool != null) {
      // Try standard close method if available
      if (oldPool instanceof AutoCloseable) {
        try {
          ((AutoCloseable) oldPool).close();
          LOG.info("Successfully closed old Hive client pool");
        } catch (Exception e) {
          LOG.warn("Failed to close old Hive client pool", e);
        }
      }

      // Additionally, try to shutdown the internal scheduler thread pool in Iceberg's
      // CachedClientPool to prevent memory leak. This is necessary because Iceberg's
      // CachedClientPool does not implement Closeable and does not properly clean up
      // its internal scheduler.
      try {
        shutdownIcebergCachedClientPoolScheduler(oldPool);
      } catch (Exception e) {
        LOG.warn(
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
        LOG.debug("clientPoolCache is null, no scheduler to shutdown");
        return;
      }

      ScheduledExecutorService executor =
          CaffeineSchedulerExtractorUtils.extractSchedulerExecutor(cache);
      if (executor != null) {
        LOG.info("Shutting down scheduler thread pool from old CachedClientPool");
        executor.shutdownNow();
      } else {
        LOG.debug("Could not extract scheduler executor from cache");
      }
    } catch (IllegalAccessException e) {
      LOG.debug("Failed to access clientPoolCache field", e);
    }
  }

  public HiveCatalog getProxy() {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);
    return (HiveCatalog) e.create();
  }
}
