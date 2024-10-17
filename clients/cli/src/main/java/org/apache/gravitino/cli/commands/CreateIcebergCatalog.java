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

package org.apache.gravitino.cli.commands;

public class CreateIcebergCatalog extends CreateCatalog {

  /**
   * Create a new Iceberg catalog.
   *
   * @param url The URL of the Gravitino server.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param provider The provider/type of catalog.
   * @param comment The catalog's comment.
   * @param metastore The Hive metastore URL.
   * @param warehouse The Iceberg warehouse.
   */
  public CreateIcebergCatalog(
      String url,
      String metalake,
      String catalog,
      String provider,
      String comment,
      String metastore,
      String warehouse) {
    super(url, metalake, catalog, provider, comment);
    properties.put("uri", metastore);
    properties.put("catalog-backend", "hive");
    properties.put("warehouse", warehouse);
  }
}
