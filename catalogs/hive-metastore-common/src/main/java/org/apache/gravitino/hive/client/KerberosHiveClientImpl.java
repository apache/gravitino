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
package org.apache.gravitino.hive.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

public class KerberosHiveClientImpl implements InvocationHandler {

  private final HiveClient delegate;
  private final UserGroupInformation realUser;

  private KerberosHiveClientImpl(HiveClient delegate, UserGroupInformation realUser) {
    this.delegate = delegate;
    this.realUser = realUser;
  }

  /**
   * Wraps a {@link HiveClient} so that all its methods are executed via {@link
   * UserGroupInformation#doAs(PrivilegedExceptionAction)} of the current user.
   *
   * <p>Callers should ensure Kerberos has been configured and the login user is set appropriately
   * (for example via keytab) before calling this method.
   */
  public static HiveClient createClient(HiveClient hiveClient) {
    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      return (HiveClient)
          Proxy.newProxyInstance(
              hiveClient.getClass().getClassLoader(),
              new Class<?>[] {HiveClient.class},
              new KerberosHiveClientImpl(hiveClient, currentUser));
    } catch (IOException e) {
      throw new RuntimeException("Failed to obtain current UserGroupInformation", e);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    // Default to the login user; optionally create a proxy user for impersonation and attach a
    // delegation token, similar to HiveProxyPlugin.
    UserGroupInformation ugiToUse = realUser;
    if (UserGroupInformation.isSecurityEnabled()) {
      String proxyKerberosPrincipalName = PrincipalUtils.getCurrentUserName();
      if (proxyKerberosPrincipalName != null && !proxyKerberosPrincipalName.isEmpty()) {
        final String finalPrincipalName = proxyKerberosPrincipalName;
        UserGroupInformation proxyUser =
            UserGroupInformation.createProxyUser(finalPrincipalName, realUser);

        // Acquire HMS delegation token for the proxy user and attach it to UGI
        String tokenStr =
            delegate.getDelegationToken(finalPrincipalName, realUser.getShortUserName());
        Token<DelegationTokenIdentifier> delegationToken = new Token<>();
        delegationToken.decodeFromUrlString(tokenStr);
        proxyUser.addToken(delegationToken);

        ugiToUse = proxyUser;
      }
    }

    try {
      return ugiToUse.doAs(
          (PrivilegedExceptionAction<Object>)
              () -> {
                try {
                  return method.invoke(delegate, args);
                } catch (InvocationTargetException ite) {
                  Throwable cause = ite.getCause();
                  if (cause instanceof Exception) {
                    throw (Exception) cause;
                  }
                  throw new RuntimeException(cause != null ? cause : ite);
                }
              });
    } catch (UndeclaredThrowableException e) {
      Throwable inner = e.getCause();
      if (inner instanceof InvocationTargetException) {
        Throwable cause = inner.getCause();
        throw cause != null ? cause : inner;
      }
      Throwable cause = inner != null ? inner : e;
      throw cause;
    } catch (IOException e) {
      Throwable cause = e.getCause();
      throw cause != null ? cause : e;
    }
  }
}
