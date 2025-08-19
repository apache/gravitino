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
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
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
      throw new RuntimeException(e);
    }
  }

  public Catalog.TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new ViewAwareTableBuilder(identifier, schema);
  }

  private class ViewAwareTableBuilder
      extends BaseMetastoreViewCatalog.BaseMetastoreViewCatalogTableBuilder {
    private final TableIdentifier identifier;

    private ViewAwareTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    public Transaction createOrReplaceTransaction() {

      boolean viewExists =
          proxy != null
              ? proxy.viewExists(this.identifier)
              : ClosableHiveCatalog.this.viewExists(this.identifier);
      if (viewExists) {
        throw new AlreadyExistsException("View with same name already exists: %s", this.identifier);
      }

      return super.createOrReplaceTransaction();
    }

    public org.apache.iceberg.Table create() {
      boolean viewExists =
          proxy != null
              ? proxy.viewExists(this.identifier)
              : ClosableHiveCatalog.this.viewExists(this.identifier);
      if (viewExists) {
        throw new AlreadyExistsException("View with same name already exists: %s", this.identifier);
      }

      return proxy.execute(() -> super.create());
    }
  }
}
