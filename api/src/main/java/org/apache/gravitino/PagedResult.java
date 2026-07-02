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
package org.apache.gravitino;

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.annotation.Evolving;

/** A paginated query result containing the total count and a page of items. */
@Evolving
public final class PagedResult<T> {

  private final long totalCount;
  private final List<T> items;

  /**
   * Creates a paginated result.
   *
   * @param totalCount The total number of matching items.
   * @param items The items in the current page.
   */
  public PagedResult(long totalCount, List<T> items) {
    this.totalCount = totalCount;
    this.items = items != null ? items : Collections.emptyList();
  }

  /**
   * Returns the total number of matching items.
   *
   * @return The total count.
   */
  public long totalCount() {
    return totalCount;
  }

  /**
   * Returns the items in the current page.
   *
   * @return The page items.
   */
  public List<T> items() {
    return items;
  }
}
