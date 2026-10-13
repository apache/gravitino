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
package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableMaintenanceContent {

  @Test
  void testPeerFieldsOnCompactionContent() {
    TimeTrigger timeTrigger = new TimeTrigger("0 0 2 * * ?");
    EventTrigger eventTrigger = new EventTrigger(true);
    Long minIntervalMs = 3_600_000L;
    Map<String, String> jobOptions = ImmutableMap.of("spark.executor.memory", "4g");
    Map<String, String> updateOptions = ImmutableMap.of("metalake", "ml");

    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1L,
                1L,
                1L,
                1L,
                10L,
                "binpack",
                "",
                ImmutableMap.of(),
                timeTrigger,
                eventTrigger,
                minIntervalMs,
                jobOptions,
                updateOptions);

    Assertions.assertTrue(content.hasPeerFields());
    Assertions.assertEquals(timeTrigger, content.timeTrigger());
    Assertions.assertEquals(eventTrigger, content.eventTrigger());
    Assertions.assertEquals(minIntervalMs, content.minIntervalMs());
    Assertions.assertEquals(jobOptions, content.jobOptions());
    Assertions.assertEquals(updateOptions, content.updateOptions());
    Assertions.assertDoesNotThrow(content::validate);

    IcebergDataCompactionContent copy =
        content.withPeerFields(timeTrigger, eventTrigger, minIntervalMs, jobOptions, null);
    Assertions.assertEquals(
        content.withPeerFields(timeTrigger, eventTrigger, minIntervalMs, jobOptions, null), copy);
    Assertions.assertNotEquals(content, copy);
  }

  @Test
  void testPeerFieldsIndependentOfTaskDefaults() {
    PolicyContent withoutPeers = PolicyContents.icebergDataCompaction();
    Assertions.assertInstanceOf(TableMaintenanceContent.class, withoutPeers);
    Assertions.assertFalse(((TableMaintenanceContent) withoutPeers).hasPeerFields());
    Assertions.assertDoesNotThrow(withoutPeers::validate);
  }
}
