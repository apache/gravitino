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
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoSystemConnector {
  @Test
  public void testSystemTablePageSourceReturnsPageOnlyOnce() throws Exception {
    Page page = new Page(0);
    try (GravitinoSystemConnector.SystemTablePageSource pageSource =
        new GravitinoSystemConnector.SystemTablePageSource(page)) {

      Assertions.assertFalse(pageSource.isFinished());
      SourcePage first = pageSource.getNextSourcePage();
      Assertions.assertNotNull(first);
      Assertions.assertSame(page, first.getPage());
      Assertions.assertTrue(pageSource.isFinished());
      Assertions.assertNull(pageSource.getNextSourcePage());
    }
  }

  @Test
  public void testSystemTablePageSourceMultipleGetNextPageCalls() throws Exception {
    Page page = new Page(0);
    try (GravitinoSystemConnector.SystemTablePageSource pageSource =
        new GravitinoSystemConnector.SystemTablePageSource(page)) {

      // First call should return the page
      SourcePage firstPage = pageSource.getNextSourcePage();
      Assertions.assertNotNull(firstPage);
      Assertions.assertSame(page, firstPage.getPage());
      Assertions.assertTrue(pageSource.isFinished());

      // Subsequent calls should return null
      Assertions.assertNull(pageSource.getNextSourcePage());
      Assertions.assertNull(pageSource.getNextSourcePage());
      Assertions.assertNull(pageSource.getNextSourcePage());
      Assertions.assertTrue(pageSource.isFinished());
    }
  }
}
