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

import io.trino.spi.Page;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoSystemConnector {
  @Test
  public void testSystemTablePageSourceReturnsPageOnlyOnce() throws Exception {
    Page page = new Page(0);
    try (GravitinoSystemConnector.SystemTablePageSource pageSource =
        new GravitinoSystemConnector.SystemTablePageSource(page)) {

      Assertions.assertFalse(pageSource.isFinished());
      Assertions.assertSame(page, pageSource.getNextPage());
      Assertions.assertTrue(pageSource.isFinished());
      Assertions.assertNull(pageSource.getNextPage());
    }
  }

  @Test
  public void testSystemTablePageSourceMultipleGetNextPageCalls() throws Exception {
    Page page = new Page(0);
    try (GravitinoSystemConnector.SystemTablePageSource pageSource =
        new GravitinoSystemConnector.SystemTablePageSource(page)) {

      // First call should return the page
      Page firstPage = pageSource.getNextPage();
      Assertions.assertNotNull(firstPage);
      Assertions.assertSame(page, firstPage);
      Assertions.assertTrue(pageSource.isFinished());

      // Subsequent calls should return null
      Assertions.assertNull(pageSource.getNextPage());
      Assertions.assertNull(pageSource.getNextPage());
      Assertions.assertNull(pageSource.getNextPage());
      Assertions.assertTrue(pageSource.isFinished());
    }
  }
}
