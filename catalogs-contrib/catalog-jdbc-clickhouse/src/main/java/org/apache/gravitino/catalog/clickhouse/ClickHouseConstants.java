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
    public static final String GRAPHITE_CONFIG = "graphite.config";

    /** Parameters for supported parameterized MergeTree engines, without outer parentheses. */
    public static final String ENGINE_PARAMETERS = "engine_parameters";

    /**
     * Read-only property that exposes ClickHouse's canonical native partition expression as
     * returned by system.tables.partition_key. It carries expressions that cannot be mapped to a
     * structured Transform (identity, year, month, or day).
     */
    public static final String PARTITION_KEY = "partition-key";
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

    /** The name of the data skipping index type for ngrambf_v1 in ClickHouse. */
    public static final String DATA_SKIPPING_NGRAMBFV1 = "ngrambf_v1";

    /** The name of the data skipping index type for tokenbf_v1 in ClickHouse. */
    public static final String DATA_SKIPPING_TOKENBFV1 = "tokenbf_v1";

    /** The name of the vector similarity data skipping index type in ClickHouse. */
    public static final String DATA_SKIPPING_VECTOR_SIMILARITY = "vector_similarity";

    /** The property key for the algorithm used by vector similarity indexes. */
    public static final String VECTOR_SIMILARITY_TYPE = "type";

    /** The property key for the distance function used by vector similarity indexes. */
    public static final String VECTOR_SIMILARITY_DISTANCE_FUNCTION = "distance_function";

    /** The property key for the vector dimension used by vector similarity indexes. */
    public static final String VECTOR_SIMILARITY_DIMENSIONS = "dimensions";

    /** The property key for HNSW vector quantization. */
    public static final String VECTOR_SIMILARITY_QUANTIZATION = "quantization";

    /** The property key for the HNSW maximum connections per layer. */
    public static final String HNSW_MAX_CONNECTIONS_PER_LAYER = "hnsw_max_connections_per_layer";

    /** The property key for the HNSW candidate list size used during construction. */
    public static final String HNSW_CANDIDATE_LIST_SIZE_FOR_CONSTRUCTION =
        "hnsw_candidate_list_size_for_construction";

    /** Property key for bloom filter size in ngrambf_v1 and tokenbf_v1 index properties. */
    public static final String BLOOM_FILTER_SIZE = "bloom_filter_size";

    /** Property key for the number of hash functions in ngrambf_v1 and tokenbf_v1 properties. */
    public static final String HASH_FUNCTIONS = "hash_functions";

    /** Property key for the random seed in ngrambf_v1 and tokenbf_v1 index properties. */
    public static final String RANDOM_SEED = "random_seed";

    /** Property key for the n-gram size in ngrambf_v1 index properties. */
    public static final String NGRAM_SIZE = "ngram_size";
  }
}
