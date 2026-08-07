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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.rest.RESTRequest;

/** Represents a request to create a catalog. */
@Getter
@EqualsAndHashCode
@ToString
public class CatalogCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private final String name;

  @JsonProperty("type")
  private final Catalog.Type type;

  @JsonProperty("provider")
  private String provider;

  @Nullable
  @JsonProperty("comment")
  private final String comment;

  @Nullable
  @JsonProperty("properties")
  private final Map<String, String> properties;

  /**
   * Constructor for CatalogCreateRequest.
   *
   * @param name The name of the catalog.
   * @param type The type of the catalog.
   * @param provider The provider of the catalog.
   * @param comment The comment for the catalog.
   * @param properties The properties for the catalog.
   */
  @JsonCreator
  public CatalogCreateRequest(
      @JsonProperty("name") String name,
      @JsonProperty("type") Catalog.Type type,
      @JsonProperty("provider") String provider,
      @JsonProperty("comment") String comment,
      @JsonProperty("properties") Map<String, String> properties) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.properties = properties;

    if (StringUtils.isNotBlank(provider)) {
      this.provider = provider;
    } else if (type != null && type.supportsManagedCatalog()) {
      this.provider = CatalogProvider.shortNameForManagedCatalog(type);
    } else {
      throw new IllegalArgumentException(
          "Provider cannot be null for catalog type "
              + type
              + " that doesn't support managed catalog");
    }

    if (this.provider != null && this.provider.equalsIgnoreCase("hadoop")) {
      // For backward compatibility, if the provider is "hadoop" (legacy case), we set the
      // provider to "fileset". This is because provider "hadoop" was previously used as a
      // "fileset" catalog. This is a special case to maintain compatibility.
      if (type != null) {
        this.provider = CatalogProvider.shortNameForManagedCatalog(type);
      } else {
        throw new IllegalArgumentException(
            "Catalog type cannot be null when provider is \"hadoop\"");
      }
    }
  }

  /**
   * Validates the fields of the request.
   *
   * @throws IllegalArgumentException if name or type are not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(type != null, "\"type\" field is required and cannot be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(provider) || type.supportsManagedCatalog(),
        "\"provider\" field is required and cannot be empty for catalog type "
            + type
            + " that doesn't support managed catalog");
  }
}
