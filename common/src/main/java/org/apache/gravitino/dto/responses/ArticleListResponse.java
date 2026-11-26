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
package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.apache.gravitino.dto.kb.ArticleSummaryDTO;
import org.apache.gravitino.kb.ArticlePage;

/** A page of visible Knowledge Base Article summaries. */
@Getter
public class ArticleListResponse extends BaseResponse {
  private final List<ArticleSummaryDTO> articles;
  private final boolean hasMore;

  /**
   * Creates a successful response from a page of matching Articles.
   *
   * @param page the visible Article summaries and continuation flag
   */
  public ArticleListResponse(ArticlePage page) {
    this(
        page.articles().stream()
            .map(ArticleSummaryDTO::from)
            .collect(ImmutableList.toImmutableList()),
        page.hasMore());
  }

  @JsonCreator
  private ArticleListResponse(
      @JsonProperty("articles") List<ArticleSummaryDTO> articles,
      @JsonProperty("hasMore") boolean hasMore) {
    super(0);
    this.articles = ImmutableList.copyOf(articles);
    this.hasMore = hasMore;
  }

  /**
   * @return the page of Article summaries and its continuation flag
   */
  public ArticlePage toPage() {
    return ArticlePage.of(ImmutableList.copyOf(articles), hasMore);
  }
}
