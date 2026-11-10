/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.common.ops;

import java.lang.reflect.Method;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.Factory;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.iceberg.catalog.Catalog;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;

/**
 * A proxy class for IcebergCatalogWrapper to handle Kerberos authentication or other cross-cutting
 * concerns.
 */
public class KerberosAwareIcebergCatalogProxy implements MethodInterceptor {
  private static final Objenesis OBJENESIS = new ObjenesisStd();

  private final IcebergCatalogWrapper target;
  private final Catalog catalog;

  public KerberosAwareIcebergCatalogProxy(IcebergCatalogWrapper target) {
    this.target = target;
    this.catalog = target.getCatalog();
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    if (catalog instanceof SupportsKerberos) {
      SupportsKerberos kerberosCatalog = (SupportsKerberos) catalog;
      return kerberosCatalog.doKerberosOperations(() -> methodProxy.invoke(target, objects));
    }

    return method.invoke(target, objects);
  }

  /**
   * Create a proxy instance that delegates every call to the wrapped target. The proxy instance is
   * allocated without running any constructor of the wrapper class, so no second catalog or
   * Kerberos client (and its ticket refresh thread) is created. Closing the proxy closes the target
   * exactly once.
   *
   * @return The proxy instance.
   */
  public IcebergCatalogWrapper getProxy() {
    Class<?> targetClass = target.getClass();
    Enhancer e = new Enhancer();
    e.setClassLoader(targetClass.getClassLoader());
    e.setSuperclass(targetClass);
    e.setCallbackType(KerberosAwareIcebergCatalogProxy.class);
    Class<?> proxyClass = e.createClass();
    Factory proxy = (Factory) OBJENESIS.newInstance(proxyClass);
    proxy.setCallbacks(new Callback[] {this});
    return (IcebergCatalogWrapper) proxy;
  }
}
