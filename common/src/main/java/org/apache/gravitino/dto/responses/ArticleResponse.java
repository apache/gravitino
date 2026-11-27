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
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.gravitino.dto.kb.ArticleDTO;

/** The current metadata of a Knowledge Base Article. */
@Getter
public class ArticleResponse extends BaseResponse {
  private final ArticleDTO article;

  /**
   * Creates a successful metadata response.
   *
   * @param article the current Article metadata
   */
  @JsonCreator
  public ArticleResponse(@JsonProperty("article") ArticleDTO article) {
    super(0);
    Preconditions.checkArgument(article != null, "article is required");
    this.article = article;
  }
}
