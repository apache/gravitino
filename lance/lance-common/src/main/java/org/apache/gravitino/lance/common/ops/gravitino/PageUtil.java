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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Pagination helper for Lance REST compatible list APIs. */
class PageUtil {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private PageUtil() {}

  static int normalizePageSize(Integer limit) {
    if (limit == null) {
      return DEFAULT_PAGE_SIZE;
    }

    Preconditions.checkArgument(limit > 0, "Page size limit must be positive");
    return limit;
  }

  static Page splitPage(List<String> sortedItems, String pageToken, int pageSize) {
    int startIndex = 0;
    if (StringUtils.isNotBlank(pageToken)) {
      try {
        startIndex = Integer.parseInt(pageToken);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid page token: " + pageToken, e);
      }
    }

    Preconditions.checkArgument(startIndex >= 0, "Page token cannot be negative: %s", pageToken);
    Preconditions.checkArgument(
        startIndex <= sortedItems.size(),
        "Page token out of range: %s, total items: %s",
        pageToken,
        sortedItems.size());

    int endIndex = Math.min(startIndex + pageSize, sortedItems.size());
    List<String> pageItems =
        startIndex == endIndex
            ? Collections.emptyList()
            : sortedItems.subList(startIndex, endIndex);
    String nextPageToken = endIndex < sortedItems.size() ? String.valueOf(endIndex) : null;
    return new Page(pageItems, nextPageToken);
  }

  static class Page {
    private final List<String> items;
    private final String nextPageToken;

    Page(List<String> items, String nextPageToken) {
      this.items = items;
      this.nextPageToken = nextPageToken;
    }

    List<String> items() {
      return items;
    }

    String nextPageToken() {
      return nextPageToken;
    }
  }
}
