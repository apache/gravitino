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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.CatalogDTO;

/** Represents a response containing catalog information. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CatalogResponse extends BaseResponse {

  @JsonProperty("catalog")
  private final CatalogDTO catalog;

  /**
   * Constructor for CatalogResponse.
   *
   * @param catalog The catalog data transfer object.
   */
  public CatalogResponse(CatalogDTO catalog) {
    super(0);
    this.catalog = catalog;
  }

  /** Default constructor for CatalogResponse. (Used for Jackson deserialization.) */
  public CatalogResponse() {
    super();
    this.catalog = null;
  }

  /**
   * Validates the response data.
   *
   * @throws IllegalArgumentException if the catalog name, type or audit is not set.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(catalog != null, "catalog must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalog.name()), "catalog 'name' must not be null and empty");
    Preconditions.checkArgument(catalog.type() != null, "catalog 'type' must not be null");
    Preconditions.checkArgument(catalog.auditInfo() != null, "catalog 'audit' must not be null");
  }
}
