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

  /** Constants for tables with the distributed engine. */
  public static final class DistributedTableConstants {
    private DistributedTableConstants() {}
    // Sharding key for the clickhouse cluster
    public static final String SHARDING_KEY = "cluster-sharding-key";
    public static final String REMOTE_DATABASE = "cluster-remote-database";
    public static final String REMOTE_TABLE = "cluster-remote-table";
  }

  /** Constants for cluster tables. */
  public static final class ClusterConstants {
    private ClusterConstants() {}

    // Name of the clickhouse cluster
    public static final String CLUSTER_NAME = "cluster-name";
    // Whether to use 'ON CLUSTER' clause when creating tables
    public static final String ON_CLUSTER = "on-cluster";
  }

  /** Table-scoped properties. */
  public static final class TableConstants {
    private TableConstants() {}

    public static final String ENGINE = "engine";
    public static final String ENGINE_UPPER = "ENGINE";
    public static final String SETTINGS_PREFIX = "settings.";
  }

  public static final class IndexConstants {
    private IndexConstants() {}

    // The name of the data skipping index type for minmax index in clickhouse.
    public static final String DATA_SKIPPING_MINMAX_VALUE = "minmax";

    // The name of the data skipping index type for bloom filter index in clickhouse.
    public static final String DATA_SKIPPING_BLOOM_FILTER = "bloom_filter";

    // The name of the data skipping index type for set index in clickhouse.
    public static final String DATA_SKIPPING_SET = "set";

    // Key for GRANULARITY in index properties (data-skipping index granularity).
    public static final String GRANULARITY = "granularity";

    // Key for max unique values (N) in set(N) data-skipping index properties.
    public static final String SET_MAX_VALUES = "set_max_values";

    // The name of the data skipping index type for ngrambf_v1 index in clickhouse.
    public static final String DATA_SKIPPING_NGRAMBFV1 = "ngrambf_v1";

    // The name of the data skipping index type for tokenbf_v1 index in clickhouse.
    public static final String DATA_SKIPPING_TOKENBFV1 = "tokenbf_v1";

    // Property key for bloom filter size in ngrambf_v1/tokenbf_v1 index properties.
    public static final String BLOOM_FILTER_SIZE = "bloom_filter_size";

    // Property key for number of hash functions in ngrambf_v1/tokenbf_v1 index properties.
    public static final String HASH_FUNCTIONS = "hash_functions";

    // Property key for random seed in ngrambf_v1/tokenbf_v1 index properties.
    public static final String RANDOM_SEED = "random_seed";

    // Property key for ngram size in ngrambf_v1 index properties.
    public static final String NGRAM_SIZE = "ngram_size";
  }
}
