/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage;

import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class HTTPLineageSinkConfig extends Config {

  public static final ConfigEntry<String> URL =
      new ConfigBuilder("url")
          .doc("The url of lineage service")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .create();

  public static final ConfigEntry<Integer> TIMEOUT =
      new ConfigBuilder("timeout_ms")
          .doc("The timeout of connect to lineage service")
          .version(ConfigConstants.VERSION_0_9_0)
          .intConf()
          .createWithDefault(Integer.valueOf(10000));

  public HTTPLineageSinkConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }
}
