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
package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.dto.responses.BaseListResponse;
import org.apache.gravitino.pagination.Page;
import org.apache.gravitino.pagination.PageRequest;

/** Builds the query parameters of paged list requests and the pages they return. */
final class PageParams {

  static final String PAGE_SIZE = "pageSize";
  static final String PAGE_TOKEN = "pageToken";

  private PageParams() {}

  /**
   * Returns the query parameters of a paged request.
   *
   * @param pageRequest The page to read.
   * @return A mutable map holding {@code pageSize} and, for a later page, {@code pageToken}.
   */
  static Map<String, String> of(PageRequest pageRequest) {
    return with(new HashMap<>(), pageRequest);
  }

  /**
   * Adds the paging query parameters to existing ones.
   *
   * @param params The existing query parameters; not modified.
   * @param pageRequest The page to read.
   * @return A new mutable map holding the existing parameters and the paging parameters.
   */
  static Map<String, String> with(Map<String, String> params, PageRequest pageRequest) {
    Preconditions.checkArgument(pageRequest != null, "pageRequest must not be null");
    Map<String, String> result = new HashMap<>(params);
    result.put(PAGE_SIZE, String.valueOf(pageRequest.pageSize()));
    pageRequest.pageToken().ifPresent(token -> result.put(PAGE_TOKEN, token));
    return result;
  }

  /**
   * Creates a page from an array of entries and the response that carried them.
   *
   * @param items The entries of the page.
   * @param response The list response.
   * @param <T> The entry type.
   * @return The page.
   */
  static <T> Page<T> page(T[] items, BaseListResponse response) {
    return page(Arrays.asList(items), response);
  }

  /**
   * Creates a page from a list of entries and the response that carried them.
   *
   * @param items The entries of the page.
   * @param response The list response.
   * @param <T> The entry type.
   * @return The page.
   */
  static <T> Page<T> page(List<T> items, BaseListResponse response) {
    return Page.of(items, response.nextPageToken());
  }
}
