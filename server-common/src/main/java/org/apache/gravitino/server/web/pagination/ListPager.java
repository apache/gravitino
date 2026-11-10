/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.server.web.pagination;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;

/**
 * Cuts one page out of a list for the REST API list endpoints.
 *
 * <p>The pager sorts the list by a unique sort key, skips the entries up to and including the key
 * carried by the page token, and authorizes the remaining entries chunk by chunk until it holds one
 * visible entry more than the page size. Authorization therefore happens before the cut: a page is
 * never short because hidden entries were dropped from it, and the next page token is returned only
 * when a visible entry remains.
 *
 * <p>Sort keys are compared with {@link String#compareTo(String)}, never with a locale collation,
 * so every server node produces the same order.
 */
public final class ListPager {

  private ListPager() {}

  /**
   * One page of a list.
   *
   * @param <T> The entry type.
   */
  public static final class Result<T> {
    private final List<T> items;
    private final String nextPageToken;

    Result(List<T> items, String nextPageToken) {
      this.items = Collections.unmodifiableList(items);
      this.nextPageToken = nextPageToken;
    }

    /**
     * @return The entries in the page, in sort-key order.
     */
    public List<T> items() {
      return items;
    }

    /**
     * @return The token for the next page, or null when no visible entry remains.
     */
    public String nextPageToken() {
      return nextPageToken;
    }
  }

  /**
   * Whether a request asks for a page. A request with neither parameter receives the full list.
   *
   * @param pageSize The {@code pageSize} query parameter, or null.
   * @param pageToken The {@code pageToken} query parameter, or null.
   * @return True if the request is paged.
   */
  public static boolean isPaged(Integer pageSize, String pageToken) {
    return pageSize != null || pageToken != null;
  }

  /**
   * Returns the configured maximum page size, {@link Configs#LIST_MAX_PAGE_SIZE}.
   *
   * @return The maximum page size.
   */
  public static int maxPageSize() {
    Config config = GravitinoEnv.getInstance().config();
    return config == null
        ? Configs.DEFAULT_LIST_MAX_PAGE_SIZE
        : config.get(Configs.LIST_MAX_PAGE_SIZE);
  }

  /**
   * Cuts one page out of a list.
   *
   * @param entries The full list, in any order. Sort keys must be unique within it.
   * @param sortKey Returns the sort key of an entry.
   * @param visible Returns the entries of a chunk the caller may see, in the order given. Called
   *     once per chunk with consecutive slices of the sorted list.
   * @param pageSize The {@code pageSize} query parameter, or null.
   * @param pageToken The {@code pageToken} query parameter, or null.
   * @param scope The scope of the list, from {@link PageToken#scopeOf(String...)}.
   * @param maxPageSize The largest page the server returns.
   * @param <T> The entry type.
   * @return The page.
   * @throws IllegalArgumentException If {@code pageSize} is below 1 or {@code pageToken} is
   *     invalid. The message names the parameter.
   */
  public static <T> Result<T> page(
      List<T> entries,
      Function<T, String> sortKey,
      Function<List<T>, List<T>> visible,
      Integer pageSize,
      String pageToken,
      String scope,
      int maxPageSize) {
    if (pageSize != null && pageSize < 1) {
      throw new IllegalArgumentException("pageSize must be at least 1, got: " + pageSize);
    }
    Preconditions.checkArgument(maxPageSize > 0, "maxPageSize must be positive");
    int size = pageSize == null ? maxPageSize : Math.min(pageSize, maxPageSize);
    String afterKey = pageToken == null ? null : PageToken.decodeKey(pageToken, scope);

    List<T> sorted = new ArrayList<>(entries);
    sorted.sort(Comparator.comparing(sortKey));

    int idx = afterKey == null ? 0 : firstIndexAfter(sorted, sortKey, afterKey);
    int want = size + 1;
    int chunk = want;
    int chunkCap = Math.max(maxPageSize, want);
    List<T> found = new ArrayList<>(want);
    while (idx < sorted.size() && found.size() < want) {
      int end = Math.min(sorted.size(), idx + chunk);
      found.addAll(visible.apply(sorted.subList(idx, end)));
      idx = end;
      chunk = (int) Math.min((long) chunk * 2, chunkCap);
    }

    if (found.size() <= size) {
      return new Result<>(found, null);
    }
    List<T> page = new ArrayList<>(found.subList(0, size));
    String next = PageToken.encode(sortKey.apply(page.get(size - 1)), scope);
    return new Result<>(page, next);
  }

  /** Returns the index of the first entry whose key sorts strictly after {@code afterKey}. */
  private static <T> int firstIndexAfter(
      List<T> sorted, Function<T, String> sortKey, String afterKey) {
    int lo = 0;
    int hi = sorted.size();
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (sortKey.apply(sorted.get(mid)).compareTo(afterKey) <= 0) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }
}
