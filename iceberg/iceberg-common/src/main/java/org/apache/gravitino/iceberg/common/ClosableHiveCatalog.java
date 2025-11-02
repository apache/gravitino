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
import java.lang.reflect.Field;
import java.util.List;
import org.apache.gravitino.iceberg.common.utils.IcebergHiveCachedClientPool;
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

    // First, close the internal HiveCatalog client pool to prevent resource leaks
    closeInternalClientPool();

    // Then close any additional resources added via addResource()
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

  /**
   * Close the internal HiveCatalog client pool using reflection. This is necessary because
   * HiveCatalog doesn't provide a public API to close its client pool. We need to avoid closing
   * IcebergHiveCachedClientPool twice (once here and once in resources list).
   */
  private void closeInternalClientPool() {
    try {
      Field clientsField = HiveCatalog.class.getDeclaredField("clients");
      clientsField.setAccessible(true);
      Object clientPool = clientsField.get(this);

      if (clientPool != null && clientPool instanceof AutoCloseable) {
        // Only close if it's NOT IcebergHiveCachedClientPool
        if (!(clientPool instanceof IcebergHiveCachedClientPool)) {
          ((AutoCloseable) clientPool).close();
          LOGGER.info(
              "Closed HiveCatalog internal client pool: {}", clientPool.getClass().getSimpleName());
        }
      }
    } catch (NoSuchFieldException e) {
      LOGGER.warn("Could not find 'clients' field in HiveCatalog, skipping cleanup", e);
    } catch (Exception e) {
      LOGGER.warn("Failed to close HiveCatalog internal client pool", e);
    }
  }
}
