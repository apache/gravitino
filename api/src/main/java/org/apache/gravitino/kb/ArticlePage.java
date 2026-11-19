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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.annotation.Evolving;

/**
 * One page of article summaries together with the total number of articles matching the request
 * filter. The total applies the same visibility filter as the page, so it never counts an article
 * the caller cannot read.
 */
@Evolving
public class ArticlePage {

  private final List<ArticleSummary> articles;
  private final int total;
  private final boolean hasMore;

  /**
   * Creates an article page.
   *
   * @param articles the articles on this page
   * @param total the number of articles matching all filters visible to the caller
   * @return the page
   */
  public static ArticlePage of(ArticleSummary[] articles, int total) {
    return new ArticlePage(
        articles == null ? ImmutableList.of() : Arrays.asList(articles), total, 0);
  }

  /**
   * Creates an article page.
   *
   * @param articles the articles on this page
   * @param total the number of articles matching all filters visible to the caller
   * @return the page
   */
  public static ArticlePage of(List<ArticleSummary> articles, int total) {
    return new ArticlePage(articles, total, 0);
  }

  /**
   * Creates a page for a zero-based offset in the visible results.
   *
   * @param articles summaries in this window
   * @param total total visible matches
   * @param offset number of preceding visible matches requested
   * @return the page
   */
  public static ArticlePage of(List<ArticleSummary> articles, int total, int offset) {
    return new ArticlePage(articles, total, offset);
  }

  /**
   * @return whether more visible matches follow the requested window
   */
  public boolean hasMore() {
    return hasMore;
  }

  private ArticlePage(List<ArticleSummary> articles, int total, int offset) {
    this.articles = articles == null ? ImmutableList.of() : ImmutableList.copyOf(articles);
    Preconditions.checkArgument(total >= 0, "total must not be negative");
    Preconditions.checkArgument(offset >= 0, "offset must not be negative");
    this.total = total;
    this.hasMore = (long) offset + this.articles.size() < total;
  }

  /**
   * @return the articles on this page
   */
  public List<ArticleSummary> articles() {
    return articles;
  }

  /**
   * @return the number of matching articles visible to the caller, independent of paging
   */
  public int total() {
    return total;
  }
}
