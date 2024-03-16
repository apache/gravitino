/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.utils.PrincipalUtils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;

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
