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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to link a model version. */
@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class ModelVersionLinkRequest implements RESTRequest {

  @Nullable
  @JsonProperty("uri")
  private String uri;

  @Nullable
  @JsonProperty("uris")
  private Map<String, String> uris;

  @JsonProperty("aliases")
  private String[] aliases;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("properties")
  private Map<String, String> properties;

  /**
   * Constructor for ModelVersionLinkRequest.
   *
   * @param uri the uri of the model version.
   * @param aliases the aliases of the model version.
   * @param comment the comment of the model version.
   * @param properties the properties of the model version.
   */
  public ModelVersionLinkRequest(
      String uri, String[] aliases, String comment, Map<String, String> properties) {
    this(uri, null, aliases, comment, properties);
  }

  /**
   * Constructor for ModelVersionLinkRequest
   *
   * @param uris the uris of the model version.
   * @param aliases the aliases of the model version.
   * @param comment the comment of the model version.
   * @param properties the properties of the model version.
   */
  public ModelVersionLinkRequest(
      Map<String, String> uris, String[] aliases, String comment, Map<String, String> properties) {
    this(null, uris, aliases, comment, properties);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    if (aliases != null && aliases.length > 0) {
      for (String alias : aliases) {
        Preconditions.checkArgument(
            StringUtils.isNotBlank(alias), "alias must not be null or empty");
        Preconditions.checkArgument(
            !NumberUtils.isCreatable(alias), "alias must not be a number or a number string");
      }
    }

    if (uris == null || uris.isEmpty()) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(uri),
          "\"uri\" field cannot be empty if uris field is null or empty");
    } else {
      uris.forEach(
          (n, u) -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(n), "URI name must not be blank");
            Preconditions.checkArgument(StringUtils.isNotBlank(u), "URI must not be blank");
          });
    }
  }
}
