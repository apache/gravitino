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
package org.apache.gravitino.pagination;

import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.annotation.Evolving;

/**
 * A request for one page of a list. The first page is requested with {@link #ofSize(int)}; each
 * following page with {@link #withToken(String)}, passing the {@link Page#nextPageToken()} of the
 * previous page.
 *
 * <p>The server may return fewer entries than the requested page size when its configured maximum
 * page size is smaller.
 */
@Evolving
public final class PageRequest {

  private final int pageSize;
  private final String pageToken;

  private PageRequest(int pageSize, String pageToken) {
    if (pageSize < 1) {
      throw new IllegalArgumentException("pageSize must be at least 1, got: " + pageSize);
    }
    this.pageSize = pageSize;
    this.pageToken = pageToken;
  }

  /**
   * Creates a request for the first page.
   *
   * @param pageSize The maximum number of entries in the page; must be at least 1.
   * @return The page request.
   */
  public static PageRequest ofSize(int pageSize) {
    return new PageRequest(pageSize, null);
  }

  /**
   * Returns a request for the page that follows the page that issued the given token.
   *
   * @param pageToken The {@link Page#nextPageToken()} of the previous page.
   * @return A new page request with the same page size and the given token.
   */
  public PageRequest withToken(String pageToken) {
    if (pageToken == null || pageToken.isEmpty()) {
      throw new IllegalArgumentException("pageToken must not be null or empty");
    }
    return new PageRequest(pageSize, pageToken);
  }

  /**
   * @return The maximum number of entries in the page.
   */
  public int pageSize() {
    return pageSize;
  }

  /**
   * @return The token of the page to read, or empty for the first page.
   */
  public Optional<String> pageToken() {
    return Optional.ofNullable(pageToken);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PageRequest)) {
      return false;
    }
    PageRequest that = (PageRequest) o;
    return pageSize == that.pageSize && Objects.equals(pageToken, that.pageToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageSize, pageToken);
  }

  @Override
  public String toString() {
    return "PageRequest{pageSize=" + pageSize + ", pageToken=" + pageToken + "}";
  }
}
