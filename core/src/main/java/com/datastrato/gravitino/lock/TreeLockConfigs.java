/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;

/** This interface is used to define tree lock configs. */
public interface TreeLockConfigs extends Configs {

  long MAX_NODE_IN_MEMORY = 10000L;
  long MIN_NODE_IN_MEMORY = 1000L;
  long CLEAN_INTERVAL_IN_SECS = 60L;

  ConfigEntry<Long> TREE_LOCK_MAX_NODE_IN_MEMORY =
      new ConfigBuilder("gravitino.lock.maxNodes")
          .doc("The maximum number of tree lock nodes to keep in memory")
          .version("0.4.0")
          .longConf()
          .createWithDefault(MAX_NODE_IN_MEMORY);

  ConfigEntry<Long> TREE_LOCK_MIN_NODE_IN_MEMORY =
      new ConfigBuilder("gravitino.lock.minNodes")
          .doc("The minimum number of tree lock nodes to keep in memory")
          .version("0.4.0")
          .longConf()
          .createWithDefault(MIN_NODE_IN_MEMORY);

  ConfigEntry<Long> TREE_LOCK_CLEAN_INTERVAL =
      new ConfigBuilder("gravitino.lock.cleanIntervalInSecs")
          .doc("The interval in seconds to clean up the stale tree lock nodes")
          .version("0.4.0")
          .longConf()
          .createWithDefault(CLEAN_INTERVAL_IN_SECS);
}
