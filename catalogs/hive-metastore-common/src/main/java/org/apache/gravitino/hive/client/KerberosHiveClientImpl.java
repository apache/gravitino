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
import java.util.Properties;
import org.apache.hadoop.security.UserGroupInformation;

public class KerberosHiveClientImpl implements InvocationHandler {

  private final HiveClient delegate;
  private final UserGroupInformation ugi;

  private KerberosHiveClientImpl(HiveClient delegate, UserGroupInformation ugi) {
    this.delegate = delegate;
    this.ugi = ugi;
  }

  /**
   * Wraps a {@link HiveClient} so that all its methods are executed via {@link
   * UserGroupInformation#doAs(PrivilegedExceptionAction)} of the current user.
   *
   * <p>Callers should ensure Kerberos has been configured and the login user is set appropriately
   * (for example via keytab) before calling this method.
   */
  public static HiveClient createClient(
      HiveClientClassLoader.HiveVersion version, UserGroupInformation ugi, Properties properties) {
    try {
      HiveClient client =
          ugi.doAs(
              (PrivilegedExceptionAction<HiveClient>)
                  () ->
                      HiveClientFactory.createHiveClientImpl(
                          version, properties, Thread.currentThread().getContextClassLoader()));
      return (HiveClient)
          Proxy.newProxyInstance(
              HiveClient.class.getClassLoader(),
              new Class<?>[] {HiveClient.class},
              new KerberosHiveClientImpl(client, ugi));

    } catch (IOException | InterruptedException ex) {
      throw new RuntimeException("Failed to create Kerberos Hive client", ex);
    }
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      return ugi.doAs(
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
