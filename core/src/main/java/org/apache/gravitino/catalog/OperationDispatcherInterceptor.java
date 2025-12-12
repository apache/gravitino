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
package org.apache.gravitino.catalog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class OperationDispatcherInterceptor implements InvocationHandler {
  private final Object dispatcher;
  private final CatalogManager catalogManager;
  private final EntityStore store;

  public OperationDispatcherInterceptor(
      Object dispatcher, CatalogManager catalogManager, EntityStore store) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
    this.store = store;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (args != null && args.length > 0) {
      NameIdentifier catalogIdent = null;
      if (args[0] instanceof NameIdentifier ident) {
        catalogIdent = NameIdentifierUtil.getCatalogIdentifier(ident);
      } else if (args[0] instanceof Namespace ns) {
        if (ns.length() >= 2) {
          catalogIdent = NameIdentifier.of(ns.level(0), ns.level(1));
        }
      }

      if (catalogIdent != null) {
        final NameIdentifier finalCatalogIdent = catalogIdent;
        // This will break logic into two compared to that in the original code.
        TreeLockUtils.doWithTreeLock(
            catalogIdent,
            LockType.READ,
            () -> {
              catalogManager.checkCatalogInUse(store, finalCatalogIdent);
              return null;
            });
      }
    }

    try {
      return method.invoke(dispatcher, args);
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }
}
