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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestPageUtil {

  private static final List<String> ITEMS = List.of("a", "b", "c");

  @Test
  void testNormalizePageSizeUsesDefaultForNullLimit() {
    assertEquals(1000, PageUtil.normalizePageSize(null));
  }

  @Test
  void testNormalizePageSizePreservesPositiveLimit() {
    assertEquals(25, PageUtil.normalizePageSize(25));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, Integer.MIN_VALUE})
  void testNormalizePageSizeRejectsNonPositiveLimit(int limit) {
    assertThrows(IllegalArgumentException.class, () -> PageUtil.normalizePageSize(limit));
  }

  @Test
  void testSplitPageReturnsFirstPageAndNextToken() {
    PageUtil.Page page = PageUtil.splitPage(ITEMS, null, 2);

    assertEquals(List.of("a", "b"), page.items());
    assertEquals("2", page.nextPageToken());
  }

  @Test
  void testSplitPageReturnsLastPageWithoutNextToken() {
    PageUtil.Page page = PageUtil.splitPage(ITEMS, "2", 2);

    assertEquals(List.of("c"), page.items());
    assertNull(page.nextPageToken());
  }

  @Test
  void testSplitPageReturnsEmptyPageForEmptyItems() {
    PageUtil.Page page = PageUtil.splitPage(List.of(), null, 2);

    assertEquals(List.of(), page.items());
    assertNull(page.nextPageToken());
  }

  @Test
  void testSplitPageReturnsEmptyPageForTokenAtEnd() {
    PageUtil.Page page = PageUtil.splitPage(ITEMS, String.valueOf(ITEMS.size()), 2);

    assertEquals(List.of(), page.items());
    assertNull(page.nextPageToken());
  }

  @Test
  void testSplitPageAvoidsOverflowForMaximumPageSize() {
    List<String> items = List.of("a", "b");
    PageUtil.Page firstPage = PageUtil.splitPage(items, null, 1);

    assertEquals(List.of("a"), firstPage.items());
    assertEquals("1", firstPage.nextPageToken());

    PageUtil.Page secondPage =
        PageUtil.splitPage(items, firstPage.nextPageToken(), Integer.MAX_VALUE);

    assertEquals(List.of("b"), secondPage.items());
    assertNull(secondPage.nextPageToken());
  }

  @Test
  void testSplitPageReturnsEmptyPageForEndTokenWithMaximumPageSize() {
    List<String> items = List.of("a", "b");
    PageUtil.Page page = PageUtil.splitPage(items, String.valueOf(items.size()), Integer.MAX_VALUE);

    assertEquals(List.of(), page.items());
    assertNull(page.nextPageToken());
  }

  @Test
  void testSplitPageRejectsInvalidPageToken() {
    assertThrows(IllegalArgumentException.class, () -> PageUtil.splitPage(ITEMS, "invalid", 2));
  }

  @Test
  void testSplitPageRejectsNegativePageToken() {
    assertThrows(IllegalArgumentException.class, () -> PageUtil.splitPage(ITEMS, "-1", 2));
  }

  @Test
  void testSplitPageRejectsOutOfRangePageToken() {
    assertThrows(IllegalArgumentException.class, () -> PageUtil.splitPage(ITEMS, "4", 2));
  }
}
