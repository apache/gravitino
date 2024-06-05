/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.authentication;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergHiveCachedClientPool;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.conf.Configuration;
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
  private String kerberosRealm;
  private final UserGroupInformation proxyUser;
  private final Map<String, String> properties;

  public HiveBackendProxy(Map<String, String> properties, HiveCatalog target) {
    this.target = target;
    this.properties = properties;
    try {
      initKerberos(properties, target.getConf());
      proxyUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
    }
  }

  private void initKerberos(Map<String, String> properties, Configuration conf) {
    try {
      KerberosClient kerberosClient = new KerberosClient(properties, conf);
      File keytabFile =
          kerberosClient.saveKeyTabFileFromUri(Long.valueOf(properties.get("catalog_uuid")));
      this.kerberosRealm = kerberosClient.login(keytabFile.getAbsolutePath());
    } catch (IOException e) {
      throw new RuntimeException("Failed to login with kerberos", e);
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

    String token = getToken();

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

  private String getToken()
      throws NoSuchFieldException, IllegalAccessException, TException, InterruptedException {
    final Field m = HiveCatalog.class.getDeclaredField("clients");
    m.setAccessible(true);
    ClientPool<IMetaStoreClient, TException> clientPool =
        (ClientPool<IMetaStoreClient, TException>) m.get(target);

    String token =
        clientPool.run(
            client ->
                client.getDelegationToken(
                    PrincipalUtils.getCurrentPrincipal().getName(), proxyUser.getShortUserName()));
    if (clientPool instanceof IcebergHiveCachedClientPool) {
      return token;
    }

    // Change the client pool to IcebergHiveCachedClientPool as client pool in iceberg
    // has the username problem.
    // TODO: we need to close the original client pool and thread pool, or it will cause memory
    //  leak.
    m.set(target, new IcebergHiveCachedClientPool(target.getConf(), properties));
    return token;
  }

  public HiveCatalog getProxy() {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);
    return (HiveCatalog) e.create();
  }
}
