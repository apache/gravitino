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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.CatalogDTO;

/** Represents a response for a list of catalogs with their information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CatalogListResponse extends BaseResponse {

  @JsonProperty("catalogs")
  private final CatalogDTO[] catalogs;

  /**
   * Creates a new CatalogListResponse.
   *
   * @param catalogs The list of catalogs.
   */
  public CatalogListResponse(CatalogDTO[] catalogs) {
    super(0);
    this.catalogs = catalogs;
  }

  /**
   * This is the constructor that is used by Jackson deserializer to create an instance of
   * CatalogListResponse.
   */
  public CatalogListResponse() {
    super();
    this.catalogs = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if name, type or audit information is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(catalogs != null, "catalogs must be non-null");
    Arrays.stream(catalogs)
        .forEach(
            catalog -> {
              Preconditions.checkArgument(
                  StringUtils.isNotBlank(catalog.name()),
                  "catalog 'name' must not be null and empty");
              Preconditions.checkArgument(
                  catalog.type() != null, "catalog 'type' must not be null");
              Preconditions.checkArgument(
                  catalog.auditInfo() != null, "catalog 'audit' must not be null");
            });
  }
}
