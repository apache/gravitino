/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.ProxyPlugin;
import com.datastrato.gravitino.utils.Executable;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

class HiveProxyPlugin implements ProxyPlugin {

  private final UserGroupInformation currentUser;
  private HiveCatalogOperations ops;

  HiveProxyPlugin() {
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

      if (UserGroupInformation.isSecurityEnabled()) {
        if (ops != null) {

          String token =
              ops.getClientPool()
                  .run(
                      client -> {
                        return client.getDelegationToken(
                            currentUser.getUserName(),
                            PrincipalUtils.getCurrentPrincipal().getName());
                      });

          Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
          delegationToken.decodeFromUrlString(token);
          delegationToken.setService(
              new Text(ops.getHiveConf().getVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE)));
          proxyUser.addToken(delegationToken);
        }
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
    this.ops = ((HiveCatalogOperations) ops);
    this.ops.setProxyPlugin(this);
  }
}
