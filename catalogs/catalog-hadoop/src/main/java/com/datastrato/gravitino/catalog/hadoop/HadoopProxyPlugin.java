/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.ProxyPlugin;
import com.datastrato.gravitino.utils.Executable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;

public class HadoopProxyPlugin implements ProxyPlugin {
  private HadoopCatalogOperations ops;
  private UserGroupInformation realUser;

  public HadoopProxyPlugin() {
    try {
      realUser = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      throw new IllegalStateException("Fail to init HadoopCatalogProxyPlugin");
    }
  }

  @Override
  public Object doAs(
      Principal principal, Executable<Object, Exception> action, Map<String, String> properties)
      throws Throwable {
    try {
      UserGroupInformation proxyUser;

      if (UserGroupInformation.isSecurityEnabled() && ops != null) {

        // The Gravitino server may use multiple KDC servers.
        // The http authentication use one KDC server, the Hadoop catalog may use another KDC
        // server.
        // The KerberosAuthenticator will remove realm of principal.
        // And then we add the realm of Hadoop catalog to the user.
        String proxyKerberosPrincipalName = principal.getName();
        if (!proxyKerberosPrincipalName.contains("@")) {
          proxyKerberosPrincipalName =
              String.format("%s@%s", proxyKerberosPrincipalName, ops.getKerberosRealm());
        }

        proxyUser = UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, realUser);
      } else {
        proxyUser = UserGroupInformation.createProxyUser(principal.getName(), realUser);
      }

      return proxyUser.doAs((PrivilegedExceptionAction<Object>) action::execute);
    } catch (UndeclaredThrowableException e) {
      Throwable innerException = e.getCause();
      if (innerException instanceof PrivilegedActionException) {
        throw innerException.getCause();
      } else if (innerException instanceof InvocationTargetException) {
        throw innerException.getCause();
      } else {
        throw innerException;
      }
    }
  }

  @Override
  public void bindCatalogOperation(CatalogOperations ops) {
    this.ops = ((HadoopCatalogOperations) ops);
    this.ops.setProxyPlugin(this);
  }
}
