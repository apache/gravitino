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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import lombok.Getter;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.json.StrictJsonStringDeserializer;
import org.apache.gravitino.kb.ArticleObjectIdentifier;

/** An asset identifier with its namespace represented as ordered components. */
@Getter
public class ArticleObjectDTO {
  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private final String type;

  @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
  private final List<String> namespace;

  @JsonDeserialize(using = StrictJsonStringDeserializer.class)
  private final String name;

  /**
   * Creates a structured identifier.
   *
   * @param type the asset type
   * @param namespace the ordered namespace components
   * @param name the unqualified asset name
   */
  @JsonCreator
  public ArticleObjectDTO(
      @JsonProperty(value = "type", required = true) String type,
      @JsonProperty(value = "namespace", required = true) List<String> namespace,
      @JsonProperty(value = "name", required = true) String name) {
    this.type = type;
    Preconditions.checkArgument(namespace != null, "Object namespace is required");
    this.namespace = Collections.unmodifiableList(new ArrayList<>(namespace));
    this.name = name;
  }

  /**
   * Creates the wire representation of an asset identifier.
   *
   * @param object the asset identifier
   * @return the structured identifier DTO
   */
  public static ArticleObjectDTO from(ArticleObjectIdentifier object) {
    return new ArticleObjectDTO(object.type().name(), object.namespace(), object.name());
  }

  /**
   * Converts an array of asset references for a create or binding edit.
   *
   * @param objects the structured references
   * @return the validated identifiers in request order
   */
  public static ArticleObjectIdentifier[] toIdentifiers(ArticleObjectDTO[] objects) {
    return Arrays.stream(objects)
        .map(ArticleObjectDTO::toIdentifier)
        .toArray(ArticleObjectIdentifier[]::new);
  }

  /**
   * Returns the asset identifier after validating its type and component count.
   *
   * @return the validated identifier
   */
  public ArticleObjectIdentifier toIdentifier() {
    Preconditions.checkArgument(type != null, "Object type is required");
    return ArticleObjectIdentifier.of(
        MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)), namespace, name);
  }
}
