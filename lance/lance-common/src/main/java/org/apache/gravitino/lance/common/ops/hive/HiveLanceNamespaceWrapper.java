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
package org.apache.gravitino.lance.common.ops.hive;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.lance.namespace.hive2.Hive2Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NamespaceWrapper} backed by the official {@code lance-namespace-hive2} implementation
 * ({@link Hive2Namespace}). A namespace maps to a Hive database and a table maps to a {@code
 * db.table} entry registered in the Hive Metastore as an external Lance table.
 *
 * <p>This wrapper owns a single {@link Hive2Namespace} instance and exposes it to the namespace and
 * table operation adapters, which translate between Gravitino's flat-parameter operation interfaces
 * and the Lance Namespace request/response model.
 */
public class HiveLanceNamespaceWrapper extends NamespaceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(HiveLanceNamespaceWrapper.class);

  /** Prefix for raw Hadoop/Hive properties forwarded into the Hadoop configuration. */
  private static final String HIVE_CONFIG_PREFIX = "hive.";

  /** {@link Hive2Namespace} config key for the client pool size. */
  private static final String HIVE2_CLIENT_POOL_SIZE = "client.pool-size";

  /** {@link Hive2Namespace} config key for the storage root used to derive default locations. */
  private static final String HIVE2_ROOT = "root";

  private Hive2Namespace delegate;
  private BufferAllocator allocator;

  private LanceNamespaceOperations namespaceOperations;
  private LanceTableOperations tableOperations;

  @VisibleForTesting
  HiveLanceNamespaceWrapper() {
    super(null);
  }

  /**
   * Create a Hive namespace wrapper.
   *
   * @param config the Lance configuration
   */
  public HiveLanceNamespaceWrapper(LanceConfig config) {
    super(config);
  }

  /**
   * Get the underlying Lance Hive 2 namespace implementation.
   *
   * @return the delegate, or null if {@link #initialize()} has not run
   */
  public Hive2Namespace getDelegate() {
    return delegate;
  }

  @Override
  protected void initialize() {
    String metastoreUris = config().get(LanceConfig.HIVE_METASTORE_URIS);
    String warehouse = config().get(LanceConfig.HIVE_WAREHOUSE);
    int poolSize = config().get(LanceConfig.HIVE_CLIENT_POOL_SIZE);

    Configuration hadoopConf = new Configuration();
    if (StringUtils.isNotBlank(metastoreUris)) {
      hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);
    }
    if (StringUtils.isNotBlank(warehouse)) {
      hadoopConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouse);
    }

    // Forward any raw hive.* properties into the Hadoop configuration.
    config()
        .getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith(HIVE_CONFIG_PREFIX)) {
                hadoopConf.set(key, value);
                LOG.info("Applying Hive config: {} = {}", key, value);
              }
            });

    Map<String, String> namespaceProperties = new HashMap<>();
    namespaceProperties.put(HIVE2_CLIENT_POOL_SIZE, String.valueOf(poolSize));
    if (StringUtils.isNotBlank(warehouse)) {
      namespaceProperties.put(HIVE2_ROOT, warehouse);
    }

    this.allocator = new RootAllocator();
    this.delegate = new Hive2Namespace();
    this.delegate.setHadoopConf(hadoopConf);
    this.delegate.initialize(namespaceProperties, allocator);

    LOG.info(
        "Hive Lance namespace backend initialized with metastore uris '{}', warehouse '{}',"
            + " pool size {}",
        metastoreUris,
        warehouse,
        poolSize);

    this.namespaceOperations = new HiveLanceNamespaceOperations(delegate);
    this.tableOperations = new HiveLanceTableOperations(delegate);
  }

  @Override
  protected LanceNamespaceOperations newNamespaceOps() {
    return namespaceOperations;
  }

  @Override
  protected LanceTableOperations newTableOps() {
    return tableOperations;
  }

  @Override
  public void close() {
    if (allocator != null) {
      try {
        allocator.close();
      } catch (Exception e) {
        LOG.warn("Error closing Arrow allocator for Hive Lance namespace backend", e);
      }
    }
  }
}
