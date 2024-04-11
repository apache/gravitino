/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.impl;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.config.ConfigEntry;
import java.util.Map;

class EventListenerConfig extends Config {
  static final ConfigEntry<String> LISTENER_NAMES =
      new ConfigBuilder(EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES)
          .doc("Gravitino event listener names, DOT is used to split multi names")
          .version(ConfigConstants.VERSION_0_5_0)
          .stringConf()
          .createWithDefault("");

  static final ConfigEntry<Integer> QUEUE_CAPACITY =
      new ConfigBuilder(EventListenerManager.GRAVITINO_EVENT_LISTENER_QUEUE_CAPACITY)
          .doc("Gravitino event listener async queue capacity")
          .version(ConfigConstants.VERSION_0_5_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(3000);

  static final ConfigEntry<Integer> DISPATCHER_JOIN_SECONDS =
      new ConfigBuilder(EventListenerManager.GRAVITINO_EVENT_LISTENER_DISPATCHER_JOIN_SECONDS)
          .doc("Gravitino async event dispatcher join seconds")
          .version(ConfigConstants.VERSION_0_5_0)
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(3);

  EventListenerConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
