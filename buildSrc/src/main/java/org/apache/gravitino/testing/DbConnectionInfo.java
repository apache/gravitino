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

package org.apache.gravitino.testing;

/**
 * Administrative connection info for a {@link SharedDbContainerService}-managed shared database
 * container, not naming any particular database on that server.
 */
public final class DbConnectionInfo {

  private final String adminJdbcUrl;
  private final String user;
  private final String password;

  /**
   * Creates a new connection info holder.
   *
   * @param adminJdbcUrl a JDBC URL that can be connected to without naming any database
   * @param user the administrative user
   * @param password the password for {@code user}
   */
  public DbConnectionInfo(String adminJdbcUrl, String user, String password) {
    this.adminJdbcUrl = adminJdbcUrl;
    this.user = user;
    this.password = password;
  }

  /** Returns the administrative JDBC URL, not naming any database. */
  public String getAdminJdbcUrl() {
    return adminJdbcUrl;
  }

  /** Returns the administrative user. */
  public String getUser() {
    return user;
  }

  /** Returns the password for {@link #getUser()}. */
  public String getPassword() {
    return password;
  }
}
