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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.json.StrictJsonStringDeserializer;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;
import org.apache.gravitino.rest.RESTRequest;

/** An ordered, atomic Article edit and its optional revision guard. */
public class ArticleUpdatesRequest implements RESTRequest {
  @Getter
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonSetter(value = "expectedRevision", nulls = Nulls.FAIL)
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private String expectedRevision;

  @JsonProperty("updates")
  @JsonSetter(nulls = Nulls.FAIL, contentNulls = Nulls.FAIL)
  private ArticleUpdateRequest[] updates;

  /**
   * Converts all updates to Article changes in request order.
   *
   * @return the ordered changes
   */
  public KnowledgeBaseArticleChange[] changes() {
    Preconditions.checkArgument(
        updates != null && updates.length > 0, "updates must contain one or more changes");
    return Arrays.stream(updates)
        .map(ArticleUpdateRequest::toChange)
        .toArray(KnowledgeBaseArticleChange[]::new);
  }

  @Override
  public void validate() {
    changes();
  }
}
