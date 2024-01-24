/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;

/** Proxy wrapper on a operation class to execute operations by impersonating given user */
public class OperationsProxy<T> implements InvocationHandler {

  private final ProxyPlugin plugin;
  private final T ops;

  private OperationsProxy(ProxyPlugin plugin, T ops) {
    this.plugin = plugin;
    this.ops = ops;
  }

  public static <T> T createProxy(T ops, ProxyPlugin plugin) {
    return (T)
        Proxy.newProxyInstance(
            ops.getClass().getClassLoader(),
            ops.getClass().getInterfaces(),
            new OperationsProxy(plugin, ops));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object object = plugin.doAs(
        PrincipalUtils.getCurrentPrincipal(),
        () -> method.invoke(ops, args),
        Collections.emptyMap());
    if (object instanceof Table) {
      return createProxy(object, plugin);
    }
    return object;
  }
}
