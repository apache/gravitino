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

import io.openlineage.server.OpenLineage.RunEvent;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestLineageSinkManager {

  @SneakyThrows
  @Test
  void testLineageSinkManager() {
    LineageSinkManager lineageSinkManager = new LineageSinkManager();
    lineageSinkManager.initialize(getLineageSinkConfig());
    lineageSinkManager.sink(getRunEvent());
    Thread.sleep(3000);
  }

  private RunEvent getRunEvent() {
    RunEvent runEvent = Mockito.mock(RunEvent.class);
    return runEvent;
  }

  private Map<String, String> getLineageSinkConfig() {
    Map<String, String> lineageSinkConfigs = new HashMap<>();
    lineageSinkConfigs.put(LineageConfig.LINEAGE_CONFIG_SINKS, "sink1,sink2");
    lineageSinkConfigs.put(
        "sink1." + LineageConfig.LINEAGE_SINK_CLASS_NAME, LineageSinkForTest.class.getName());
    lineageSinkConfigs.put("sink1.a", "a");
    lineageSinkConfigs.put(
        "sink2." + LineageConfig.LINEAGE_SINK_CLASS_NAME, LineageSinkForTest.class.getName());
    lineageSinkConfigs.put("sink2.a", "b");
    return lineageSinkConfigs;
  }
}
