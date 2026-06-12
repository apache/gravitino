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

package org.apache.gravitino.spark.connector.authorization;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.connector.catalog.Table;

/** Creates Spark table proxies that retain denied Gravitino privilege information. */
public final class AuthorizationTableProxy {

  private AuthorizationTableProxy() {}

  /**
   * Wraps a Spark table while preserving all public interfaces implemented by the table.
   *
   * @param delegate the underlying Spark table
   * @param tableIdentifier the fully qualified Gravitino table identifier
   * @param requiredPrivileges the privileges required to load the table
   * @param forbiddenException the original authorization failure
   * @return a table proxy carrying the authorization failure
   */
  public static Table wrap(
      Table delegate,
      String tableIdentifier,
      Set<Privilege.Name> requiredPrivileges,
      ForbiddenException forbiddenException) {
    Set<Class<?>> interfaces = new LinkedHashSet<>();
    collectPublicInterfaces(delegate.getClass(), interfaces);
    interfaces.add(SupportsRequiredPrivileges.class);

    InvocationHandler handler =
        new AuthorizationInvocationHandler(
            delegate, tableIdentifier, ImmutableSet.copyOf(requiredPrivileges), forbiddenException);
    return (Table)
        Proxy.newProxyInstance(
            AuthorizationTableProxy.class.getClassLoader(),
            interfaces.toArray(new Class<?>[0]),
            handler);
  }

  private static void collectPublicInterfaces(Class<?> type, Set<Class<?>> interfaces) {
    if (type == null) {
      return;
    }
    for (Class<?> implementedInterface : type.getInterfaces()) {
      if (Modifier.isPublic(implementedInterface.getModifiers())) {
        interfaces.add(implementedInterface);
        collectPublicInterfaces(implementedInterface, interfaces);
      }
    }
    collectPublicInterfaces(type.getSuperclass(), interfaces);
  }

  private static class AuthorizationInvocationHandler implements InvocationHandler {

    private final Table delegate;
    private final String tableIdentifier;
    private final Set<Privilege.Name> requiredPrivileges;
    private final ForbiddenException forbiddenException;

    private AuthorizationInvocationHandler(
        Table delegate,
        String tableIdentifier,
        Set<Privilege.Name> requiredPrivileges,
        ForbiddenException forbiddenException) {
      this.delegate = delegate;
      this.tableIdentifier = tableIdentifier;
      this.requiredPrivileges = requiredPrivileges;
      this.forbiddenException = forbiddenException;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass().equals(SupportsRequiredPrivileges.class)) {
        switch (method.getName()) {
          case "tableIdentifier":
            return tableIdentifier;
          case "requiredPrivileges":
            return requiredPrivileges;
          case "forbiddenException":
            return forbiddenException;
          default:
            throw new UnsupportedOperationException("Unsupported marker method: " + method);
        }
      }

      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
