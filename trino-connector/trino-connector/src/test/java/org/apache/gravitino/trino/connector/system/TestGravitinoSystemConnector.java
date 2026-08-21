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
package org.apache.gravitino.trino.connector.system;

import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoSystemConnector {
  @Test
  public void testSystemTablePageSourceReturnsPageOnlyOnce() throws Exception {
    Page page = new Page(0);
    try (GravitinoSystemConnector.SystemTablePageSource pageSource =
        Mockito.mock(
            GravitinoSystemConnector.SystemTablePageSource.class,
            Mockito.withSettings()
                .useConstructor(page)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS))) {

      Assertions.assertFalse(pageSource.isFinished());
      Assertions.assertSame(page, pageSource.nextPage());
      Assertions.assertTrue(pageSource.isFinished());
      Assertions.assertNull(pageSource.nextPage());
    }
  }

  @Test
  public void testSystemTablePageSourceMultipleGetNextPageCalls() throws Exception {
    Page page = new Page(0);
    try (GravitinoSystemConnector.SystemTablePageSource pageSource =
        Mockito.mock(
            GravitinoSystemConnector.SystemTablePageSource.class,
            Mockito.withSettings()
                .useConstructor(page)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS))) {

      // First call should return the page
      Page firstPage = pageSource.nextPage();
      Assertions.assertNotNull(firstPage);
      Assertions.assertSame(page, firstPage);
      Assertions.assertTrue(pageSource.isFinished());

      // Subsequent calls should return null
      Assertions.assertNull(pageSource.nextPage());
      Assertions.assertNull(pageSource.nextPage());
      Assertions.assertNull(pageSource.nextPage());
      Assertions.assertTrue(pageSource.isFinished());
    }
  }

  @Test
  public void testSplitIsRemotelyAccessibleUntilTheCoordinatorIsKnown() {
    HostAddress previous = null;
    try {
      GravitinoSystemConnector.Split.setCoordinatorAddress(null);
      GravitinoSystemConnector.Split split = mockSplit();

      // Without a known coordinator the split keeps the original, unpinned behaviour.
      Assertions.assertTrue(split.isRemotelyAccessible());
      Assertions.assertTrue(split.getAddresses().isEmpty());
    } finally {
      GravitinoSystemConnector.Split.setCoordinatorAddress(previous);
    }
  }

  @Test
  public void testSplitIsPinnedToTheCoordinator() {
    try {
      // The system tables are only populated on the coordinator, so a split must never be
      // scheduled onto a worker.
      HostAddress coordinator = HostAddress.fromParts("127.0.0.1", 8080);
      GravitinoSystemConnector.Split.setCoordinatorAddress(coordinator);
      GravitinoSystemConnector.Split split = mockSplit();

      Assertions.assertFalse(split.isRemotelyAccessible());
      Assertions.assertEquals(List.of(coordinator), split.getAddresses());
    } finally {
      GravitinoSystemConnector.Split.setCoordinatorAddress(null);
    }
  }

  private static final SchemaTableName TABLE_NAME = new SchemaTableName("system", "catalog");

  private static GravitinoSystemConnector.Split mockSplit() {
    // Mocked rather than subclassed: ConnectorSplit's abstract members differ across the
    // supported Trino SPI versions, and this test compiles against all of them.
    return Mockito.mock(
        GravitinoSystemConnector.Split.class,
        Mockito.withSettings()
            .useConstructor(TABLE_NAME)
            .defaultAnswer(Mockito.CALLS_REAL_METHODS));
  }
}
