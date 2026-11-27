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
package org.apache.gravitino.dto.glossary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.gravitino.glossary.TermLink;
import org.apache.gravitino.json.JsonUtils.StrictStringDeserializer;

/** A labeled glossary documentation link. */
@Getter
public class TermLinkDTO {
  @Nullable private final String label;
  private final String url;

  /**
   * Creates a link with an optional label.
   *
   * @param label The optional display label.
   * @param url The link URL.
   */
  @JsonCreator
  public TermLinkDTO(
      @JsonDeserialize(using = StrictStringDeserializer.class) @JsonProperty("label") @Nullable
          String label,
      @JsonDeserialize(using = StrictStringDeserializer.class) @JsonProperty("url") String url) {
    this.label = label;
    this.url = url;
  }

  /**
   * Returns the validated API link.
   *
   * @return The validated API link.
   */
  public TermLink toLink() {
    return TermLink.of(label, url);
  }
}
