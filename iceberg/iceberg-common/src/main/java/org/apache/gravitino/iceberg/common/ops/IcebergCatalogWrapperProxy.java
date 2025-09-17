/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.common.ops;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.ClosableHiveCatalog;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;
import org.apache.gravitino.iceberg.common.utils.IcebergHiveCachedClientPool;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.thrift.TException;

public class IcebergCatalogWrapperProxy implements MethodInterceptor {

  private final IcebergCatalogWrapper target;

  boolean needProxy = false;
  private KerberosClient kerberosClient;
  private ClientPool<IMetaStoreClient, TException> newClientPool;

  public IcebergCatalogWrapperProxy(IcebergCatalogWrapper target) {
    this.target = target;
    Catalog catalog = target.catalog;

    // Need special handling for HiveCatalog.
    if (catalog instanceof HiveCatalog) {
      HiveCatalog hiveCatalog = (HiveCatalog) catalog;

      try {
        Map<String, String> properties =
            (Map<String, String>) FieldUtils.readField(hiveCatalog, "catalogProperties", true);
        AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);
        if (authenticationConfig.isImpersonationEnabled()) {
          needProxy = true;
          kerberosClient = (KerberosClient) ((ClosableHiveCatalog) catalog).getResources().get(0);
          newClientPool = resetIcebergHiveClientPool(hiveCatalog, properties);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to create HiveBackendProxy", e);
      }
    }
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    if (needProxy) {
      final String finalPrincipalName;
      String proxyKerberosPrincipalName = PrincipalUtils.getCurrentPrincipal().getName();
      if (!proxyKerberosPrincipalName.contains("@")) {
        finalPrincipalName =
            String.format("%s@%s", proxyKerberosPrincipalName, kerberosClient.getRealm());
      } else {
        finalPrincipalName = proxyKerberosPrincipalName;
      }
      UserGroupInformation proxyUser = UserGroupInformation.getCurrentUser();

      UserGroupInformation realUser =
          UserGroupInformation.createProxyUser(
              finalPrincipalName, UserGroupInformation.getCurrentUser());

      String token =
          newClientPool.run(
              client ->
                  client.getDelegationToken(finalPrincipalName, proxyUser.getShortUserName()));

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

    return methodProxy.invoke(target, objects);
  }

  private ClientPool<IMetaStoreClient, TException> resetIcebergHiveClientPool(
      HiveCatalog catalog, Map<String, String> catalogProperties)
      throws IllegalAccessException, NoSuchFieldException {
    final Field m = HiveCatalog.class.getDeclaredField("clients");
    m.setAccessible(true);

    // TODO: we need to close the original client pool and thread pool, or it will cause memory
    //  leak.
    ClientPool<IMetaStoreClient, TException> newClientPool =
        new IcebergHiveCachedClientPool(catalog.getConf(), catalogProperties);
    m.set(catalog, newClientPool);
    return newClientPool;
  }

  public IcebergCatalogWrapper getProxy(IcebergConfig config) {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);

    Class<?>[] argClass = new Class[] {IcebergConfig.class};
    return (IcebergCatalogWrapper) e.create(argClass, new Object[] {config});
  }
}
