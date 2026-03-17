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

package org.apache.gravitino.maintenance.optimizer.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStatisticsInputContent {

  @Test
  void testFromFilePath() {
    StatisticsInputContent content = StatisticsInputContent.fromFilePath("/tmp/stats.jsonl");

    Assertions.assertTrue(content.hasFilePath());
    Assertions.assertEquals("/tmp/stats.jsonl", content.filePath());
    Assertions.assertFalse(content.hasPayload());
    Assertions.assertNull(content.payload());
  }

  @Test
  void testFromPayload() {
    StatisticsInputContent content = StatisticsInputContent.fromPayload("{\"k\":\"v\"}");

    Assertions.assertTrue(content.hasPayload());
    Assertions.assertEquals("{\"k\":\"v\"}", content.payload());
    Assertions.assertFalse(content.hasFilePath());
    Assertions.assertNull(content.filePath());
  }

  @Test
  void testFromFilePathRejectBlank() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StatisticsInputContent.fromFilePath("  "));
  }

  @Test
  void testFromPayloadRejectBlank() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StatisticsInputContent.fromPayload("  "));
  }
}
