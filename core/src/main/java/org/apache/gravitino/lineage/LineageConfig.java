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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

public class LineageConfig extends Config {
  public static final String LINEAGE_CONFIG_PREFIX = "gravitino.lineage.";
  public static final String LINEAGE_CONFIG_SINKS = "sinks";
  public static final String LINEAGE_SINK_CLASS_NAME = "sink-class";

  public static final ConfigEntry<String> SOURCE =
      new ConfigBuilder("source")
          .doc("The source of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault("http");

  public static final ConfigEntry<String> PROCESSOR =
      new ConfigBuilder("processor")
          .doc("The processor of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault("http");

  public static final ConfigEntry<String> SINKS =
      new ConfigBuilder(LINEAGE_CONFIG_SINKS)
          .doc("The sinks of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault("log");

  public LineageConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public String source() {
    return get(SOURCE);
  }

  public String processor() {
    return get(PROCESSOR);
  }

  public List<String> sinks() {
    String sinks = get(SINKS);
    return Arrays.asList(sinks);
  }
}
