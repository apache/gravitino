/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.authentication.kerberos;

import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.FileSystemCatalog;

/**
 * Proxy class for FilesystemCatalog to support kerberos authentication. We can also make
 * FilesystemCatalog as a generic type and pass it as a parameter to the constructor.
 */
public class FilesystemBackendProxy implements MethodInterceptor {

  private final FileSystemCatalog target;
  private final String kerberosRealm;
  private final UserGroupInformation proxyUser;

  public FilesystemBackendProxy(FileSystemCatalog target, String kerberosRealm) {
    this.target = target;
    this.kerberosRealm = kerberosRealm;
    try {
      proxyUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get current user", e);
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

  public FileSystemCatalog getProxy() {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);
    return (FileSystemCatalog) e.create();
  }
}
