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
package org.apache.gravitino.connector;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import org.apache.gravitino.utils.PrincipalUtils;

/** Proxy wrapper on an operation class to execute operations by impersonating given user */
public class OperationsProxy<T> implements InvocationHandler {

  private final ProxyPlugin plugin;
  private final T ops;

  private OperationsProxy(ProxyPlugin plugin, T ops) {
    this.plugin = plugin;
    this.ops = ops;
  }

  public static <T> T createProxy(T ops, ProxyPlugin plugin) {
    if (!(ops instanceof CatalogOperations) && !(ops instanceof TableOperations)) {
      throw new IllegalArgumentException(
          "Method only supports the type of CatalogOperations or TableOperations");
    }
    if (ops instanceof CatalogOperations) {
      plugin.bindCatalogOperation((CatalogOperations) ops);
    }
    return createProxyInternal(ops, plugin, ops.getClass().getInterfaces());
  }

  private static <T> T createProxyInternal(T ops, ProxyPlugin plugin, Class<?>[] interfaces) {
    return (T)
        Proxy.newProxyInstance(
            ops.getClass().getClassLoader(), interfaces, new OperationsProxy(plugin, ops));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    return plugin.doAs(
        PrincipalUtils.getCurrentPrincipal(),
        () -> method.invoke(ops, args),
        Collections.emptyMap());
  }
}
