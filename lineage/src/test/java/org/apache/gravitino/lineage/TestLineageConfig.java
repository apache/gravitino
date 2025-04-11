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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.lineage.sink.LineageLogSink;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLineageConfig {

  @Test
  void testLineageSource() {
    // default config with HTTP source
    LineageConfig lineageConfig = new LineageConfig(ImmutableMap.of());
    Assertions.assertEquals(LineageConfig.LINEAGE_HTTP_SOURCE_NAME, lineageConfig.source());
    Assertions.assertEquals(
        LineageConfig.LINEAGE_HTTP_SOURCE_CLASS_NAME, lineageConfig.sourceClass());

    // config with custom source
    lineageConfig =
        new LineageConfig(
            ImmutableMap.of(
                LineageConfig.LINEAGE_CONFIG_SOURCE,
                "source1",
                "source1." + LineageConfig.LINEAGE_SOURCE_CLASS_NAME,
                "test-class"));
    Assertions.assertEquals("source1", lineageConfig.source());
    Assertions.assertEquals("test-class", lineageConfig.sourceClass());

    LineageConfig lineageConfig2 =
        new LineageConfig(ImmutableMap.of(LineageConfig.LINEAGE_CONFIG_SOURCE, "source2"));

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class, () -> lineageConfig2.sourceClass());
  }

  @Test
  void testGetSinkConfigs() {
    // default config with log sink
    LineageConfig lineageConfig = new LineageConfig(ImmutableMap.of());
    Map<String, String> sinkConfigs = lineageConfig.getSinkConfigs();
    String sinks = sinkConfigs.get(LineageConfig.LINEAGE_CONFIG_SINKS);
    Assertions.assertEquals(LineageConfig.LINEAGE_LOG_SINK_NAME, sinks);
    String className =
        sinkConfigs.get(
            LineageConfig.LINEAGE_LOG_SINK_NAME + "." + LineageConfig.LINEAGE_SINK_CLASS_NAME);
    Assertions.assertEquals(LineageLogSink.class.getName(), className);
    String capacity = sinkConfigs.get(LineageConfig.LINEAGE_SINK_QUEUE_CAPACITY);
    Assertions.assertEquals(
        LineageConfig.LINEAGE_SINK_QUEUE_CAPACITY_DEFAULT, Integer.parseInt(capacity));

    // config multi sinks
    Map<String, String> config2 =
        ImmutableMap.of(
            LineageConfig.LINEAGE_CONFIG_SINKS,
            "sink1,sink2",
            "sink1." + LineageConfig.LINEAGE_SINK_CLASS_NAME,
            "test-class",
            "sink2." + LineageConfig.LINEAGE_SINK_CLASS_NAME,
            "test-class2");
    lineageConfig = new LineageConfig(config2);
    sinkConfigs = lineageConfig.getSinkConfigs();
    sinks = sinkConfigs.get(LineageConfig.LINEAGE_CONFIG_SINKS);
    Assertions.assertEquals("sink1,sink2", sinks);
    Assertions.assertEquals(
        "test-class", sinkConfigs.get("sink1." + LineageConfig.LINEAGE_SINK_CLASS_NAME));
    Assertions.assertEquals(
        "test-class2", sinkConfigs.get("sink2." + LineageConfig.LINEAGE_SINK_CLASS_NAME));

    // test missing sink1 class name
    Map<String, String> config3 =
        ImmutableMap.of(
            LineageConfig.LINEAGE_CONFIG_SINKS,
            "sink1,sink2",
            "sink2." + LineageConfig.LINEAGE_SINK_CLASS_NAME,
            "test-class2");

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> {
          LineageConfig lineageConfig1 = new LineageConfig(config3);
          lineageConfig1.getSinkConfigs();
        });
  }
}
