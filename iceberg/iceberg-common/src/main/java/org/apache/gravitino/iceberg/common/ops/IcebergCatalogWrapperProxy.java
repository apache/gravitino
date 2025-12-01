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
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.iceberg.catalog.Catalog;

public class IcebergCatalogWrapperProxy implements MethodInterceptor {
  private final IcebergCatalogWrapper target;
  private final Catalog catalog;

  public IcebergCatalogWrapperProxy(IcebergCatalogWrapper target) {
    this.target = target;
    this.catalog = target.catalog;
  }

  @Override
  public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy)
      throws Throwable {
    if (catalog instanceof SupportsKerberos) {
      SupportsKerberos kerberosCatalog = (SupportsKerberos) catalog;
      return kerberosCatalog.doKerberosOperations(
          target.getIcebergConfig().getIcebergCatalogProperties(),
          () -> methodProxy.invoke(target, objects));
    }

    return method.invoke(target, objects);
  }

  public IcebergCatalogWrapper getProxy(IcebergConfig config) {
    Enhancer e = new Enhancer();
    e.setClassLoader(target.getClass().getClassLoader());
    e.setSuperclass(target.getClass());
    e.setCallback(this);

    Class<?>[] argClass = new Class[] {IcebergConfig.class};
    return (IcebergCatalogWrapper) e.create(argClass, new Object[] {config});
  }
}
