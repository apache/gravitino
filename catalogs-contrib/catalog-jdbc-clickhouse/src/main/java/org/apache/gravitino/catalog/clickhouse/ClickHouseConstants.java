/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.clickhouse;

public class ClickHouseConstants {

  /** Cluster-scoped properties. */
  public static final class DistributedOrClusterConstants {
    private DistributedOrClusterConstants() {}

    // Name of the clickhouse cluster
    public static final String NAME = "cluster-name";
    // Whether to use 'ON CLUSTER' clause when creating tables
    public static final String ON_CLUSTER = "on-cluster";
    // Sharding key for the clickhouse cluster
    public static final String SHARDING_KEY = "cluster-sharding-key";
    public static final String REMOTE_DATABASE = "cluster-remote-database";
    public static final String REMOTE_TABLE = "cluster-remote-table";
  }

  /** Table-scoped properties. */
  public static final class TableConstants {
    private TableConstants() {}

    public static final String ENGINE = "engine";
    public static final String ENGINE_UPPER = "ENGINE";
    public static final String SETTINGS_PREFIX = "settings.";
  }

  public static final String CLUSTER_NAME = DistributedOrClusterConstants.NAME;
  public static final String ON_CLUSTER = DistributedOrClusterConstants.ON_CLUSTER;
  public static final String CLUSTER_SHARDING_KEY = DistributedOrClusterConstants.SHARDING_KEY;
  public static final String CLUSTER_REMOTE_DATABASE =
      DistributedOrClusterConstants.REMOTE_DATABASE;
  public static final String CLUSTER_REMOTE_TABLE = DistributedOrClusterConstants.REMOTE_TABLE;

  public static final String GRAVITINO_CLICKHOUSE_ENGINE_NAME = TableConstants.ENGINE;
  public static final String CLICKHOUSE_ENGINE_NAME = TableConstants.ENGINE_UPPER;
  public static final String SETTINGS_PREFIX = TableConstants.SETTINGS_PREFIX;
}
