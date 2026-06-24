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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * A {@link ClientPoolImpl} of Hive Metastore clients. Adapted from Apache Iceberg for Hive 2.x.
 *
 * <p>The {@link #newClient()} call is the seam that a future mTLS Hive client PR will modify.
 */
public class HiveClientPool extends ClientPoolImpl<IMetaStoreClient, TException> {

  private final HiveConf hiveConf;

  /**
   * Create a Hive Metastore client pool.
   *
   * @param poolSize the maximum number of clients held by the pool
   * @param conf the Hadoop configuration used to derive the {@link HiveConf}
   */
  public HiveClientPool(int poolSize, Configuration conf) {
    // Do not allow retry by default as we rely on RetryingMetaStoreClient.
    super(poolSize, TTransportException.class, false);
    this.hiveConf = new HiveConf(conf, HiveClientPool.class);
    this.hiveConf.addResource(conf);
  }

  @Override
  protected IMetaStoreClient newClient() {
    try {
      // Hive 2.x specific client creation - no dynamic reflection needed.
      return RetryingMetaStoreClient.getProxy(
          hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClient.class.getName());
    } catch (MetaException e) {
      throw new HiveMetaException(e, "Failed to connect to Hive Metastore");
    } catch (Throwable t) {
      if (t.getMessage() != null
          && t.getMessage().contains("Another instance of Derby may have already booted")) {
        throw new HiveMetaException(
            t,
            "Failed to start an embedded metastore because embedded "
                + "Derby supports only one client at a time. To fix this, use a metastore that"
                + " supports multiple clients.");
      }

      throw new HiveMetaException(t, "Failed to connect to Hive Metastore");
    }
  }

  @Override
  protected IMetaStoreClient reconnect(IMetaStoreClient client) {
    try {
      client.close();
      client.reconnect();
    } catch (MetaException e) {
      throw new HiveMetaException(e, "Failed to reconnect to Hive Metastore");
    }
    return client;
  }

  @Override
  protected boolean isConnectionException(Exception e) {
    return super.isConnectionException(e)
        || (e instanceof MetaException
            && e.getMessage()
                .contains("Got exception: org.apache.thrift.transport.TTransportException"));
  }

  @Override
  protected void close(IMetaStoreClient client) {
    client.close();
  }
}
