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
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NamespaceWrapper} backed by a Hive Metastore. A namespace maps to a Hive database and a
 * table maps to a {@code db.table} entry registered in the metastore as an external Lance table.
 */
public class HiveLanceNamespaceWrapper extends NamespaceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(HiveLanceNamespaceWrapper.class);

  /** Prefix for raw Hadoop/Hive properties forwarded into the Hadoop configuration. */
  private static final String HIVE_CONFIG_PREFIX = "hive.";

  private HiveClientPool clientPool;
  private String root;

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
   * Get the Hive Metastore client pool.
   *
   * @return the client pool, or null if {@link #initialize()} has not run
   */
  public HiveClientPool getClientPool() {
    return clientPool;
  }

  /**
   * Get the storage root (warehouse) location used to derive default database/table locations.
   *
   * @return the configured root location without a trailing slash
   */
  public String getRoot() {
    return root;
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

    this.root = HiveUtil.makeQualified(warehouse);
    this.clientPool = new HiveClientPool(poolSize, hadoopConf);

    LOG.info(
        "Hive Lance namespace backend initialized with metastore uris '{}', warehouse '{}',"
            + " pool size {}",
        metastoreUris,
        warehouse,
        poolSize);

    this.namespaceOperations = new HiveLanceNamespaceOperations(this);
    this.tableOperations = new HiveLanceTableOperations(this);
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
    if (clientPool != null) {
      try {
        clientPool.close();
      } catch (Exception e) {
        LOG.warn("Error closing Hive Metastore client pool", e);
      }
    }
  }
}
