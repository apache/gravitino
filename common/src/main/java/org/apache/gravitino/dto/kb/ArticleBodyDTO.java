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
package org.apache.gravitino.dto.kb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.util.Locale;
import lombok.Getter;
import org.apache.gravitino.json.StrictJsonStringDeserializer;
import org.apache.gravitino.kb.ArticleBody;

/** The text of an Article and its stored markup format. */
@Getter
public class ArticleBodyDTO {
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private final String format;

  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private final String content;

  /**
   * Creates a wire-format body.
   *
   * @param format the markup format
   * @param content the stored text
   */
  @JsonCreator
  public ArticleBodyDTO(
      @JsonProperty(value = "format", required = true) String format,
      @JsonProperty(value = "content", required = true) String content) {
    this.format = format;
    this.content = content;
  }

  /**
   * Converts a stored body to its wire representation.
   *
   * @param body the Article body
   * @return the body DTO
   */
  public static ArticleBodyDTO from(ArticleBody body) {
    return new ArticleBodyDTO(body.format().name(), body.content());
  }

  /**
   * Returns the validated Article body.
   *
   * @return the validated body
   */
  public ArticleBody toBody() {
    Preconditions.checkArgument(format != null, "Article body format is required");
    return ArticleBody.of(ArticleBody.Format.valueOf(format.toUpperCase(Locale.ROOT)), content);
  }
}
