/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.utils.PrincipalUtils;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;

/** Proxy wrapper on CatalogOperations to execute operations by impersonating given user */
public class CatalogOperationsProxy implements InvocationHandler {

  private final CatalogProxyPlugin plugin;
  private final CatalogOperations ops;

  private CatalogOperationsProxy(CatalogProxyPlugin plugin, CatalogOperations ops) {
    this.plugin = plugin;
    this.ops = ops;
  }

  public static <T extends CatalogOperations> T getProxy(T ops, CatalogProxyPlugin plugin) {
    return (T)
        Proxy.newProxyInstance(
            ops.getClass().getClassLoader(),
            ops.getClass().getInterfaces(),
            new CatalogOperationsProxy(plugin, ops));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    return plugin.doAs(
        PrincipalUtils.getCurrentPrincipal(),
        () -> method.invoke(ops, args),
        Collections.emptyMap());
  }
}
