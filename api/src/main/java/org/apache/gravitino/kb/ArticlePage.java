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

package org.apache.gravitino.kb;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.annotation.Evolving;

/** One page of article summaries visible to the caller. */
@Evolving
public class ArticlePage {

  private final List<ArticleSummary> articles;
  private final boolean hasMore;

  /**
   * Creates an article page.
   *
   * @param articles the summaries on this page
   * @param hasMore whether another matching article is visible after this page
   * @return the page
   */
  public static ArticlePage of(ArticleSummary[] articles, boolean hasMore) {
    return of(articles == null ? ImmutableList.of() : Arrays.asList(articles), hasMore);
  }

  /**
   * Creates an article page.
   *
   * @param articles the summaries on this page
   * @param hasMore whether another matching article is visible after this page
   * @return the page
   */
  public static ArticlePage of(List<ArticleSummary> articles, boolean hasMore) {
    return new ArticlePage(articles, hasMore);
  }

  private ArticlePage(List<ArticleSummary> articles, boolean hasMore) {
    this.articles = articles == null ? ImmutableList.of() : ImmutableList.copyOf(articles);
    this.hasMore = hasMore;
  }

  /**
   * @return the summaries in this page
   */
  public List<ArticleSummary> articles() {
    return articles;
  }

  /**
   * @return whether another visible match follows this page
   */
  public boolean hasMore() {
    return hasMore;
  }
}
