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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.gravitino.iceberg.common.utils.IcebergHiveCachedClientPool;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.thrift.TException;

/**
 * Proxy class for HiveCatalog to support kerberos authentication. We can also make HiveCatalog as a
 * generic type and pass it as a parameter to the constructor.
 */
public class HiveBackendProxy implements MethodInterceptor {

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
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException("Failed to reset IcebergHiveClientPool", e);
    }
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {

    String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();
    if (!proxyKerberosPrincipalName.contains("@")) {
      proxyKerberosPrincipalName =
          String.format("%s@%s", proxyKerberosPrincipalName, kerberosRealm);
    }

    UserGroupInformation realUser =
        UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, proxyUser);

    String token =
        newClientPool.run(
            client ->
                client.getDelegationToken(
                    PrincipalUtils.getCurrentPrincipal().getName(), proxyUser.getShortUserName()));

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
      throws IllegalAccessException, NoSuchFieldException {
    final Field m = HiveCatalog.class.getDeclaredField("clients");
    m.setAccessible(true);

    // TODO: we need to close the original client pool and thread pool, or it will cause memory
    //  leak.
    ClientPool<IMetaStoreClient, TException> newClientPool =
        new IcebergHiveCachedClientPool(target.getConf(), properties);
    m.set(target, newClientPool);
    return newClientPool;
  }

  public HiveCatalog getProxy() {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);
    return (HiveCatalog) e.create();
  }
}
