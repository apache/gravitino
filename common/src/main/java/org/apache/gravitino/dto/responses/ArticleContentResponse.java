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
import org.apache.gravitino.dto.kb.ArticleBodyDTO;
import org.apache.gravitino.kb.ArticleContent;

/** The stored Article body and editing token read from one snapshot. */
@Getter
public class ArticleContentResponse extends BaseResponse {
  private final ArticleBodyDTO body;
  private final String revisionToken;

  /**
   * Creates a successful response from the current content.
   *
   * @param content the body and revision token from a single snapshot
   */
  public ArticleContentResponse(ArticleContent content) {
    this(ArticleBodyDTO.from(content.body()), content.revisionToken());
  }

  @JsonCreator
  private ArticleContentResponse(
      @JsonProperty("body") ArticleBodyDTO body,
      @JsonProperty("revisionToken") String revisionToken) {
    super(0);
    Preconditions.checkArgument(body != null, "body is required");
    Preconditions.checkArgument(
        revisionToken != null && !revisionToken.isEmpty(), "revisionToken is required");
    this.body = body;
    this.revisionToken = revisionToken;
  }
}
