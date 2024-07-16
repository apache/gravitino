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
package org.apache.gravitino.catalog.hive;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.ProxyPlugin;
import org.apache.gravitino.utils.Executable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

class HiveProxyPlugin implements ProxyPlugin {

  private final UserGroupInformation realUser;
  private HiveCatalogOperations ops;

  HiveProxyPlugin() {
    try {
      realUser = UserGroupInformation.getCurrentUser();
    } catch (IOException ioe) {
      throw new IllegalStateException("Fail to init HiveCatalogProxyPlugin");
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
        // The http authentication use one KDC server, the Hive catalog may use another KDC server.
        // The KerberosAuthenticator will remove realm of principal.
        // And then we add the realm of Hive catalog to the user.
        String proxyKerberosPrincipalName = principal.getName();
        if (!proxyKerberosPrincipalName.contains("@")) {
          proxyKerberosPrincipalName =
              String.format("%s@%s", proxyKerberosPrincipalName, ops.getKerberosRealm());
        }

        proxyUser = UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, realUser);

        String token =
            ops.getClientPool()
                .run(
                    client -> {
                      return client.getDelegationToken(principal.getName(), realUser.getUserName());
                    });

        Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
        delegationToken.decodeFromUrlString(token);
        delegationToken.setService(
            new Text(ops.getHiveConf().getVar(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE)));

        proxyUser.addToken(delegationToken);
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
    this.ops = ((HiveCatalogOperations) ops);
    this.ops.setProxyPlugin(this);
  }
}
