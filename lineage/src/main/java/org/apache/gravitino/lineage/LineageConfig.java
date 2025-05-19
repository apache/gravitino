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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.lineage.processor.NoopProcessor;
import org.apache.gravitino.lineage.sink.LineageLogSink;
import org.apache.gravitino.lineage.source.HTTPLineageSource;

public class LineageConfig extends Config {

  public static final String LINEAGE_CONFIG_PREFIX = "gravitino.lineage.";
  public static final String LINEAGE_CONFIG_SINKS = "sinks";
  public static final String LINEAGE_SINK_QUEUE_CAPACITY = "sinkQueueCapacity";
  public static final String LINEAGE_CONFIG_SOURCE = "source";
  public static final String LINEAGE_SOURCE_CLASS_NAME = "sourceClass";
  public static final String LINEAGE_PROCESSOR_CLASS_NAME = "processorClass";
  public static final String LINEAGE_SINK_CLASS_NAME = "sinkClass";
  public static final String LINEAGE_HTTP_SOURCE_CLASS_NAME = HTTPLineageSource.class.getName();

  public static final String LINEAGE_LOG_SINK_NAME = "log";
  public static final String LINEAGE_HTTP_SOURCE_NAME = "http";

  @VisibleForTesting static final int LINEAGE_SINK_QUEUE_CAPACITY_DEFAULT = 10000;

  private static final Splitter splitter = Splitter.on(",");

  public static final ConfigEntry<String> SOURCE_NAME =
      new ConfigBuilder(LINEAGE_CONFIG_SOURCE)
          .doc("The name of lineage event source")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault(LINEAGE_HTTP_SOURCE_NAME);

  public static final ConfigEntry<String> PROCESSOR_CLASS =
      new ConfigBuilder(LINEAGE_PROCESSOR_CLASS_NAME)
          .doc("The class name of lineage event processor")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault(NoopProcessor.class.getName());

  public static final ConfigEntry<String> SINKS =
      new ConfigBuilder(LINEAGE_CONFIG_SINKS)
          .doc("The sinks of lineage event")
          .version(ConfigConstants.VERSION_0_9_0)
          .stringConf()
          .createWithDefault(LINEAGE_LOG_SINK_NAME);

  public static final ConfigEntry<Integer> SINK_QUEUE_CAPACITY =
      new ConfigBuilder(LINEAGE_SINK_QUEUE_CAPACITY)
          .doc("The capacity of the total lineage sink queue")
          .version(ConfigConstants.VERSION_0_9_0)
          .intConf()
          .createWithDefault(LINEAGE_SINK_QUEUE_CAPACITY_DEFAULT);

  public LineageConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public String source() {
    return get(SOURCE_NAME);
  }

  public String sourceClass() {
    if (source().equals(LINEAGE_HTTP_SOURCE_NAME)) {
      return LINEAGE_HTTP_SOURCE_CLASS_NAME;
    }
    String sourceConfig = source() + "." + LINEAGE_SOURCE_CLASS_NAME;
    String sourceClass = getRawString(sourceConfig);
    Preconditions.checkArgument(StringUtils.isNotBlank(sourceClass), sourceConfig + " is not set");
    return sourceClass;
  }

  public String processorClass() {
    return get(PROCESSOR_CLASS);
  }

  public Map<String, String> getSinkConfigs() {
    List<String> sinks = sinks();

    Map<String, String> config = getAllConfig();
    Map<String, String> m = new HashMap(config);

    String sinkString = get(SINKS);
    if (!m.containsKey(LINEAGE_CONFIG_SINKS)) {
      m.put(LINEAGE_CONFIG_SINKS, sinkString);
    }

    if (!m.containsKey(LINEAGE_SINK_QUEUE_CAPACITY)) {
      m.put(LINEAGE_SINK_QUEUE_CAPACITY, String.valueOf(get(SINK_QUEUE_CAPACITY)));
    }

    String logClassConfigKey =
        LineageConfig.LINEAGE_LOG_SINK_NAME + "." + LineageConfig.LINEAGE_SINK_CLASS_NAME;
    if (sinks.contains(LINEAGE_LOG_SINK_NAME) && !config.containsKey(logClassConfigKey)) {
      m.put(logClassConfigKey, LineageLogSink.class.getName());
    }

    sinks.stream()
        .forEach(
            sinkName -> {
              String sinkClassConfig = sinkName + "." + LineageConfig.LINEAGE_SINK_CLASS_NAME;
              Preconditions.checkArgument(
                  m.containsKey(sinkClassConfig), sinkClassConfig + " is not set");
            });

    return m;
  }

  public List<String> sinks() {
    String sinks = get(SINKS);
    return splitter
        .omitEmptyStrings()
        .trimResults()
        .splitToStream(sinks)
        .collect(Collectors.toList());
  }
}
