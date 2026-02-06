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

import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class ClickHouseConfig {

  public static final boolean DEFAULT_CK_ON_CLUSTER = false;

  // Constants part
  public static final ConfigEntry<String> CK_CLUSTER_NAME =
      new ConfigBuilder(ClusterConstants.NAME)
          .doc("Cluster name for ClickHouse distributed tables")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .checkValue(StringUtils::isNotEmpty, "Cluster name cannot be empty")
          .create();

  public static final ConfigEntry<Boolean> CK_ON_CLUSTER =
      new ConfigBuilder(ClusterConstants.ON_CLUSTER)
          .doc("Whether to use 'ON CLUSTER' clause when creating tables in ClickHouse")
          .version(ConfigConstants.VERSION_1_2_0)
          .booleanConf()
          .createWithDefault(DEFAULT_CK_ON_CLUSTER);

  public static final ConfigEntry<String> CK_CLUSTER_SHARDING_KEY =
      new ConfigBuilder(DistributedTableConstants.SHARDING_KEY)
          .doc("Sharding key for ClickHouse distributed tables")
          .version(ConfigConstants.VERSION_1_2_0)
          .stringConf()
          .create();
}
