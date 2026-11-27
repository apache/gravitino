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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Getter;
import org.apache.gravitino.kb.ArticleTermBinding;

/** The name and current publication status of a visible bound Term. */
@Getter
public class ArticleTermDTO {
  private final String name;

  @JsonSerialize(using = ToStringSerializer.class)
  private final ArticleTermBinding.Status status;

  /**
   * Creates a Term binding preview.
   *
   * @param name the Term name
   * @param status the current Term status
   */
  @JsonCreator
  public ArticleTermDTO(
      @JsonProperty("name") String name, @JsonProperty("status") ArticleTermBinding.Status status) {
    this.name = name;
    this.status = status;
  }

  /**
   * Creates a preview of a bound Term.
   *
   * @param term the visible Term binding
   * @return the Term preview
   */
  public static ArticleTermDTO from(ArticleTermBinding term) {
    return new ArticleTermDTO(term.name(), term.status());
  }
}
