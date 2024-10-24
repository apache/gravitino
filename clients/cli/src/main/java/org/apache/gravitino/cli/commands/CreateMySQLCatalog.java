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

public class CreateMySQLCatalog extends CreateCatalog {

  /**
   * Create a new MySQL catalog.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param provider The provider/type of catalog.
   * @param comment The catalog's comment.
   * @param jdbcurl The MySQL JDBC URL.
   * @param user The MySQL user.
   * @param password The MySQL user's password.
   */
  public CreateMySQLCatalog(
      String url,
      boolean ignoreVersions,
      String metalake,
      String catalog,
      String provider,
      String comment,
      String jdbcurl,
      String user,
      String password) {
    super(url, ignoreVersions, metalake, catalog, provider, comment);
    properties.put("jdbc-url", jdbcurl);
    properties.put("jdbc-user", user);
    properties.put("jdbc-password", password);
    properties.put("jdbc-driver", "com.mysql.cj.jdbc.Driver");
  }
}
