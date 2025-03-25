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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.utils.MapUtils;

public class LineageConfig extends Config {

  public static final String LINEAGE_CONFIG_PREFIX = "gravitino.lineage.";
  public static final String LINEAGE_CONFIG_SINKS = "sinks";
  public static final String LINEAGE_SINK_CLASS_NAME = "sink-class";
  public static final String LINEAGE_HTTP_SOURCE_CLASS_NAME = "org.apache.gravitino.lineage.HTTPLineageSource";

  public static final String LINEAGE_LOG_SINK_NAME = "log";

  private static final Splitter splitter = Splitter.on(",");

  public static final ConfigEntry<String> SOURCE =
      new ConfigBuilder("source")
          .doc("The source of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault(LINEAGE_HTTP_SOURCE_CLASS_NAME);

  public static final ConfigEntry<String> PROCESSOR =
      new ConfigBuilder("processor")
          .doc("The processor of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault(NoopProcessor.class.getName());

  public static final ConfigEntry<String> SINKS =
      new ConfigBuilder(LINEAGE_CONFIG_SINKS)
          .doc("The sinks of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault(LINEAGE_LOG_SINK_NAME);

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

  public Map<String, String> getLineageConfigMap() {
    List<String> sinks = sinks();
    String logSinkClass =
        LineageConfig.LINEAGE_LOG_SINK_NAME + "." + LineageConfig.LINEAGE_SINK_CLASS_NAME;
    Map<String, String> config = getAllConfig();
    if ((!sinks.contains(LINEAGE_LOG_SINK_NAME)) || config.containsKey(logSinkClass)) {
      return config;
    }

    Map m = new HashMap(config);
    m.put(logSinkClass, LineageLogSinker.class.getName());
    return m;
  }

  public List<String> sinks() {
    String sinks = get(SINKS);
    return splitter
        .omitEmptyStrings()
        .trimResults()
        .splitToStream(sinks).collect(Collectors.toList());
  }
}
