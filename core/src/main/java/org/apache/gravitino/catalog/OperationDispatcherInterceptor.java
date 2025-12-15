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

/**
 * {@code OperationDispatcherInterceptor} is an invocation handler that intercepts method calls to
 * an operation dispatcher to perform catalog usage checks before proceeding with the actual method
 * invocation.
 *
 * <p>Note: This interceptor will only intercept methods in
 *
 * <p>SchemaDispatcher
 *
 * <p>TableDispatch
 *
 * <p>FilesetDispatch
 *
 * <p>ModelDispatch
 *
 * <p>TopicDispatch
 *
 * <p>PartitionDispatch
 */
public class OperationDispatcherInterceptor implements InvocationHandler {
  private final Object dispatcher;
  private final CatalogManager catalogManager;
  private final EntityStore store;

  /**
   * An {@link InvocationHandler} implementation that intercepts method calls on dispatcher objects
   * in the Gravitino catalog system. This class is used as part of the dynamic proxy pattern to
   * wrap dispatcher instances, enabling pre-processing logic such as catalog existence checks and
   * tree-based locking before delegating the actual method invocation to the underlying dispatcher.
   *
   * <p>For each intercepted method call, if the first argument is a {@link NameIdentifier} or
   * {@link Namespace}, the interceptor extracts the catalog identifier and acquires a read lock on
   * the catalog using {@link TreeLockUtils}. It then checks if the catalog is in use via the {@link
   * CatalogManager}. Only after these checks and locks does it invoke the original method on the
   * dispatcher.
   *
   * <p>This mechanism ensures that all dispatcher operations are performed safely and consistently
   * with respect to catalog state and concurrency requirements.
   */
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
        // Note: In this implementation, the catalog-in-use check is performed separately
        // under a tree lock before invoking the main operation. In the original code,
        // this check may have been performed as part of a single, monolithic operation.
        // This separation ensures that the catalog's state is validated under the appropriate
        // lock, improving thread safety and consistency. However, it introduces a trade-off:
        // the check and the main operation are not atomic with respect to each other, so there
        // is a small window where the catalog's state could change between the check and the
        // operation. This approach was chosen to avoid holding locks during potentially
        // long-running operations, balancing safety and performance.
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
