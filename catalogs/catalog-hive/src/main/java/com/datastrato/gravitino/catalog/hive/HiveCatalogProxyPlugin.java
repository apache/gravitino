/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.catalog.CatalogProxyPlugin;
import com.datastrato.gravitino.utils.Executable;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import org.apache.hadoop.security.UserGroupInformation;

class HiveCatalogProxyPlugin implements CatalogProxyPlugin {

  private final UserGroupInformation currentUser;

  HiveCatalogProxyPlugin() {
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      throw new IllegalStateException("Fail to init HiveCatalogProxyPlugin");
    }
  }

  @Override
  public Object doAs(
      Principal principal, Executable<Object, Exception> action, Map<String, String> properties)
      throws Throwable {
    try {
      UserGroupInformation proxyUser =
          UserGroupInformation.createProxyUser(
              PrincipalUtils.getCurrentPrincipal().getName(), currentUser);
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
}
