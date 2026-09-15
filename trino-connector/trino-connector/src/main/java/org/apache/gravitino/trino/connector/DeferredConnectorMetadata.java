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
package org.apache.gravitino.trino.connector;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;

/** Lazily obtains native metadata without authenticating metadata-only management queries. */
final class DeferredConnectorMetadata {
  private final Function<ConnectorSession, ConnectorMetadata> factory;
  private final ConnectorSession querySession;
  @Nullable private ConnectorMetadata delegate;
  @Nullable private ConnectorSession pendingBegin;
  private boolean closed;

  private DeferredConnectorMetadata(
      ConnectorSession querySession, Function<ConnectorSession, ConnectorMetadata> factory) {
    this.querySession = Objects.requireNonNull(querySession, "querySession");
    this.factory = Objects.requireNonNull(factory, "factory");
  }

  static ConnectorMetadata create(
      ConnectorSession querySession, Function<ConnectorSession, ConnectorMetadata> factory) {
    DeferredConnectorMetadata handler = new DeferredConnectorMetadata(querySession, factory);
    return (ConnectorMetadata)
        Proxy.newProxyInstance(
            ConnectorMetadata.class.getClassLoader(),
            new Class<?>[] {ConnectorMetadata.class},
            handler::invoke);
  }

  @Nullable
  private synchronized Object invoke(Object proxy, Method method, @Nullable Object[] args)
      throws Throwable {
    if (method.getDeclaringClass() == Object.class) {
      switch (method.getName()) {
        case "toString":
          return "DeferredConnectorMetadata";
        case "hashCode":
          return System.identityHashCode(proxy);
        case "equals":
          return proxy == args[0];
        default:
          throw new UnsupportedOperationException(method.getName());
      }
    }
    if (method.getName().equals("cleanupQuery")) {
      closed = true;
      pendingBegin = null;
      if (delegate == null) {
        return null;
      }
    } else {
      if (closed) {
        throw new IllegalStateException("Metadata query is already closed");
      }
      if (method.getName().equals("beginQuery") && delegate == null) {
        pendingBegin = (ConnectorSession) args[0];
        return null;
      }
      if (delegate == null) {
        ConnectorSession currentSession = querySession;
        if (args != null) {
          for (Object arg : args) {
            if (arg instanceof ConnectorSession) {
              currentSession = (ConnectorSession) arg;
              break;
            }
          }
        }
        ConnectorMetadata metadata =
            Objects.requireNonNull(factory.apply(currentSession), "native metadata");
        if (pendingBegin != null) {
          metadata.beginQuery(currentSession);
        }
        pendingBegin = null;
        delegate = metadata;
      }
    }
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
