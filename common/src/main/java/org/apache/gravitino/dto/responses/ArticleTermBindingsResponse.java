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
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.gravitino.dto.kb.ArticleTermDTO;
import org.apache.gravitino.kb.TermBindingPage;

/** A page of readable Terms bound to an Article. */
@Getter
public class ArticleTermBindingsResponse extends BaseResponse {
  private final List<ArticleTermDTO> terms;
  private final boolean hasMore;

  /**
   * Creates a successful response from a page of Terms.
   *
   * @param page the visible bindings and continuation flag
   */
  public ArticleTermBindingsResponse(TermBindingPage page) {
    this(
        page.terms().stream().map(ArticleTermDTO::from).collect(Collectors.toList()),
        page.hasMore());
  }

  @JsonCreator
  private ArticleTermBindingsResponse(
      @JsonProperty("terms") List<ArticleTermDTO> terms, @JsonProperty("hasMore") boolean hasMore) {
    super(0);
    this.terms = ImmutableList.copyOf(terms);
    this.hasMore = hasMore;
  }

  /**
   * @return the page of Term bindings and its continuation flag
   */
  public TermBindingPage toPage() {
    return new TermBindingPage(ImmutableList.copyOf(terms), hasMore);
  }
}
