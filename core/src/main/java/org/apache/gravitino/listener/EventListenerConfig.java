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

package org.apache.gravitino.listener;

import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

class EventListenerConfig extends Config {
  static final ConfigEntry<String> LISTENER_NAMES =
      new ConfigBuilder(EventListenerManager.GRAVITINO_EVENT_LISTENER_NAMES)
          .doc("Gravitino event listener names, comma is utilized to separate multiple names")
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
