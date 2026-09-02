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
package org.apache.gravitino.iceberg.service.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/** Response for the Gravitino Iceberg REST catalog discovery endpoint. */
public class IcebergCatalogListResponse {

  private final CatalogInfo[] catalogs;

  /**
   * Creates a catalog list response.
   *
   * @param catalogs catalogs served by the Iceberg REST server
   */
  @JsonCreator
  public IcebergCatalogListResponse(@JsonProperty("catalogs") CatalogInfo[] catalogs) {
    this.catalogs = Preconditions.checkNotNull(catalogs, "catalogs must not be null");
  }

  /**
   * Returns the catalogs served by the Iceberg REST server.
   *
   * @return catalog information
   */
  public CatalogInfo[] catalogs() {
    return catalogs;
  }

  /** Catalog information advertised by the Iceberg REST server. */
  public static class CatalogInfo {

    private final String name;

    /**
     * Creates catalog information.
     *
     * @param name catalog name accepted as an Iceberg REST {@code warehouse}
     */
    @JsonCreator
    public CatalogInfo(@JsonProperty("name") String name) {
      this.name = Preconditions.checkNotNull(name, "name must not be null");
    }

    /**
     * Returns the advertised catalog name.
     *
     * @return catalog name
     */
    public String name() {
      return name;
    }
  }
}
