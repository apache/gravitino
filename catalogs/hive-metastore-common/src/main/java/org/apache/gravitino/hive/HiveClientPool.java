/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.hive;

import java.util.Properties;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.hive.client.HiveClient;
import org.apache.gravitino.hive.client.HiveClientFactory;
import org.apache.gravitino.utils.ClientPoolImpl;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// hive-metastore/src/main/java/org/apache/iceberg/hive/HiveClientPool.java

/** Represents a client pool for managing connections to an Apache Hive Metastore service. */
public class HiveClientPool extends ClientPoolImpl<HiveClient, GravitinoRuntimeException> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveClientPool.class);
  private final Properties properties;

  /**
   * Creates a new HiveClientPool with the specified pool size and configuration.
   *
   * @param poolSize The number of clients in the pool.
   * @param properties The configuration used to initialize the Hive Metastore clients.
   */
  public HiveClientPool(int poolSize, Properties properties) {
    // Do not allow retry by default as we rely on RetryingHiveClient
    super(poolSize, GravitinoRuntimeException.class, false);
    this.properties = properties;
  }

  @Override
  protected HiveClient newClient() {
    try {
      String user = PrincipalUtils.getCurrentUserName();
      return HiveClientFactory.createHiveClient(properties);
    } catch (Exception e) {
      LOG.error("Failed to connect to Hive Metastore", e);
      throw new GravitinoRuntimeException(e, "Failed to connect to Hive Metastore");
    }
  }

  @Override
  protected HiveClient reconnect(HiveClient client) {
    LOG.warn("Reconnecting to Hive Metastore");
    return client;
  }

  @Override
  protected boolean isConnectionException(Exception e) {
    return false;
  }

  @Override
  protected void close(HiveClient client) {
    LOG.info("Closing Hive Metastore client");
  }
}
