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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.annotation.Evolving;

/**
 * One page of a list.
 *
 * @param <T> The type of the entries in the page.
 */
@Evolving
public interface Page<T> {

  /**
   * @return The entries in this page, in the list's stable order.
   */
  List<T> items();

  /**
   * @return The token for the next page, or empty when no further entry remains.
   */
  Optional<String> nextPageToken();

  /**
   * Creates a page.
   *
   * @param items The entries in the page.
   * @param nextPageToken The token for the next page, or null when no further entry remains.
   * @param <T> The type of the entries in the page.
   * @return The page.
   */
  static <T> Page<T> of(List<T> items, String nextPageToken) {
    List<T> copy = items == null ? Collections.emptyList() : Collections.unmodifiableList(items);
    Optional<String> token =
        nextPageToken == null || nextPageToken.isEmpty()
            ? Optional.empty()
            : Optional.of(nextPageToken);
    return new Page<T>() {
      @Override
      public List<T> items() {
        return copy;
      }

      @Override
      public Optional<String> nextPageToken() {
        return token;
      }

      @Override
      public String toString() {
        return "Page{items=" + copy + ", nextPageToken=" + token.orElse(null) + "}";
      }
    };
  }
}
