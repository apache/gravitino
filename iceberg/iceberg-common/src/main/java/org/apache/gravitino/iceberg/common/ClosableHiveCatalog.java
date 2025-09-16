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

package org.apache.gravitino.iceberg.common;

import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClosableHiveCatalog is a wrapper class to wrap Iceberg HiveCatalog to do some clean-up work like
 * closing resources.
 */
public class ClosableHiveCatalog extends HiveCatalog implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosableHiveCatalog.class);

  private final List<Closeable> resources = Lists.newArrayList();

  private ClosableHiveCatalog proxy;

  public void setProxy(ClosableHiveCatalog proxy) {
    this.proxy = proxy;
  }

  public ClosableHiveCatalog() {
    super();
  }

  public void addResource(Closeable resource) {
    resources.add(resource);
  }

  @Override
  public void close() throws IOException {
    // Do clean up work here. We need a mechanism to close the HiveCatalog; however, HiveCatalog
    // doesn't implement the Closeable interface.
    resources.forEach(
        resource -> {
          try {
            if (resource != null) {
              resource.close();
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to close resource: {}", resource, e);
          }
        });
  }

  @FunctionalInterface
  interface Executable<R> {
    R run() throws Exception;
  }

  public <R> R execute(Executable<R> runnable) {
    try {
      return runnable.run();
    } catch (Exception e) {
      LOGGER.error("Failed to execute runnable: {}", runnable, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }

      throw new RuntimeException(e);
    }
  }

  public Catalog.TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    ExtendedViewAwareTableBuilderExtended builder =
        new ExtendedViewAwareTableBuilderExtended(identifier, schema);
    ViewAwareTableBuilderProxy proxy =
        new ViewAwareTableBuilderProxy(builder, this.proxy != null ? this.proxy : this);
    ExtendedViewAwareTableBuilderExtended r =
        proxy.getProxy(new Object[] {this, identifier, schema});
    return r;
  }

  public static class ViewAwareTableBuilderProxy implements MethodInterceptor {
    private final ExtendedViewAwareTableBuilderExtended target;
    private final ClosableHiveCatalog closableHiveCatalogProxy;

    public ViewAwareTableBuilderProxy(
        ExtendedViewAwareTableBuilderExtended target,
        ClosableHiveCatalog closableHiveCatalogProxy) {
      this.target = target;
      this.closableHiveCatalogProxy = closableHiveCatalogProxy;
    }

    @Override
    public Object intercept(
        Object o,
        java.lang.reflect.Method method,
        Object[] objects,
        net.sf.cglib.proxy.MethodProxy methodProxy)
        throws Throwable {

      // Methods from Object class are not proxied.
      if (method.getDeclaringClass() == Object.class) {
        return methodProxy.invoke(target, objects);
      }

      // Special handling for "with" methods to return the proxy itself for method chaining.
      if (method.getReturnType().isAssignableFrom(o.getClass())
          && method.getName().startsWith("with")) {
        methodProxy.invoke(target, objects);
        return o;
      }

      if (closableHiveCatalogProxy != null) {
        return closableHiveCatalogProxy.execute(
            () -> {
              try {
                return methodProxy.invoke(target, objects);
              } catch (Throwable t) {
                LOGGER.error(
                    "Failed to invoke method: {} in ViewAwareTableBuilderProxy ", method, t);
                if (t instanceof RuntimeException) {
                  throw (RuntimeException) t;
                }

                throw new RuntimeException(t);
              }
            });
      }
      return methodProxy.invoke(target, objects);
    }

    public ExtendedViewAwareTableBuilderExtended getProxy(Object[] args) {
      Enhancer e = new Enhancer();
      e.setClassLoader(target.getClass().getClassLoader());
      e.setSuperclass(target.getClass());
      e.setCallback(this);

      Class<?>[] argClass =
          new Class[] {ClosableHiveCatalog.class, TableIdentifier.class, Schema.class};
      return (ExtendedViewAwareTableBuilderExtended) e.create(argClass, args);
    }
  }

  public class ExtendedViewAwareTableBuilderExtended
      extends ExtendedBaseMetastoreViewCatalogTableBuilder {
    public ExtendedViewAwareTableBuilderExtended(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
    }
  }

  protected class ExtendedBaseMetastoreViewCatalogTableBuilder
      extends BaseMetastoreCatalogTableBuilder {
    private final TableIdentifier identifier;

    public ExtendedBaseMetastoreViewCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    @Override
    public Transaction replaceTransaction() {
      if (viewExists(identifier)) {
        throw new AlreadyExistsException("View with same name already exists: %s", identifier);
      }

      return super.replaceTransaction();
    }
  }
}
