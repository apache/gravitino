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
package org.apache.gravitino.spark.connector.jdbc.doris;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

/** Tests the fail-closed governed Doris write policy. */
public class TestDorisWritePolicy35 {

  @Test
  void testDefaultsToDisabledRejectMode() {
    DorisWritePolicy35 policy = DorisWritePolicy35.from(ImmutableMap.of());

    assertFalse(policy.enabled());
    assertFalse(policy.allowsTruncate());
    assertTrue(policy.forcedConnectorOptions().isEmpty());
  }

  @Test
  void testForcesReviewedStreamLoadOptions() {
    DorisWritePolicy35 policy =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH));

    assertTrue(policy.enabled());
    assertFalse(policy.allowsTruncate());
    assertEquals("stream_load", policy.forcedConnectorOptions().get("doris.sink.mode"));
    assertEquals("true", policy.forcedConnectorOptions().get("doris.sink.enable-2pc"));
    assertEquals("true", policy.forcedConnectorOptions().get("doris.sink.properties.strict_mode"));
    assertEquals("0", policy.forcedConnectorOptions().get("doris.max.filter.ratio"));
    assertEquals("false", policy.forcedConnectorOptions().get("doris.write.schemaless"));
    assertEquals("false", policy.forcedConnectorOptions().get("doris.sink.auto-redirect"));
  }

  @Test
  void testTruncateRequiresBatchAndExactValues() {
    DorisWritePolicy35 truncate =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH,
                DorisConnectorConstants35.GRAVITINO_WRITE_OVERWRITE_MODE,
                DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE));

    assertTrue(truncate.allowsTruncate());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWritePolicy35.from(
                ImmutableMap.of(
                    DorisConnectorConstants35.GRAVITINO_WRITE_OVERWRITE_MODE,
                    DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DorisWritePolicy35.from(
                ImmutableMap.of(DorisConnectorConstants35.GRAVITINO_WRITE_MODE, "streaming")));
  }
}
