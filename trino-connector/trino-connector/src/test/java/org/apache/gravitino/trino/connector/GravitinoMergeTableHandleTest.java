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
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests for GravitinoMergeTableHandle functionality */
public class GravitinoMergeTableHandleTest {

  @Test
  public void testGravitinoMergeTableHandle() {
    // Mock the internal merge handle
    ConnectorMergeTableHandle mockInternalHandle = Mockito.mock(ConnectorMergeTableHandle.class);
    ConnectorTableHandle mockTableHandle = Mockito.mock(ConnectorTableHandle.class);
    Mockito.when(mockInternalHandle.getTableHandle()).thenReturn(mockTableHandle);

    // Create GravitinoMergeTableHandle
    GravitinoMergeTableHandle gravitinoMergeHandle =
        new GravitinoMergeTableHandle(mockInternalHandle);

    // Verify the handle works correctly
    assertNotNull(gravitinoMergeHandle.getInternalHandle());
    assertEquals(mockInternalHandle, gravitinoMergeHandle.getInternalHandle());
    assertEquals(mockTableHandle, gravitinoMergeHandle.getTableHandle());
    assertNotNull(gravitinoMergeHandle.getHandleString());
  }

  @Test
  public void testSerialization() {
    // Mock the internal merge handle
    ConnectorMergeTableHandle mockInternalHandle = Mockito.mock(ConnectorMergeTableHandle.class);
    ConnectorTableHandle mockTableHandle = Mockito.mock(ConnectorTableHandle.class);
    Mockito.when(mockInternalHandle.getTableHandle()).thenReturn(mockTableHandle);

    // Create GravitinoMergeTableHandle
    GravitinoMergeTableHandle originalHandle = new GravitinoMergeTableHandle(mockInternalHandle);

    // Test JSON serialization
    String handleString = originalHandle.getHandleString();
    assertNotNull(handleString);

    // Test deserialization (would need proper JSON in real scenario)
    // For this basic test, we just verify the string is not empty
    assert !handleString.isEmpty();
  }
}
