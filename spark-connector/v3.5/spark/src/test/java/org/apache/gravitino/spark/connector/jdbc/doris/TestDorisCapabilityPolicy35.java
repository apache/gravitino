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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.junit.jupiter.api.Test;

/** Tests that the governed Doris facade exposes only certified capabilities. */
public class TestDorisCapabilityPolicy35 {

  @Test
  void testCapabilityMatrix() {
    assertEquals(
        ImmutableSet.of(TableCapability.BATCH_READ),
        DorisCapabilityPolicy35.from(DorisWritePolicy35.disabled()).tableCapabilities());

    DorisWritePolicy35 append =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH));
    assertEquals(
        ImmutableSet.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE),
        DorisCapabilityPolicy35.from(append).tableCapabilities());

    DorisWritePolicy35 truncate =
        DorisWritePolicy35.from(
            ImmutableMap.of(
                DorisConnectorConstants35.GRAVITINO_WRITE_MODE,
                DorisConnectorConstants35.WRITE_BATCH,
                DorisConnectorConstants35.GRAVITINO_WRITE_OVERWRITE_MODE,
                DorisConnectorConstants35.WRITE_OVERWRITE_TRUNCATE));
    assertEquals(
        ImmutableSet.of(
            TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE),
        DorisCapabilityPolicy35.from(truncate).tableCapabilities());
    assertTrue(DorisCapabilityPolicy35.from(truncate).allowsTableWrites());
    assertFalse(
        DorisCapabilityPolicy35.from(truncate)
            .tableCapabilities()
            .contains(TableCapability.STREAMING_WRITE));
  }
}
