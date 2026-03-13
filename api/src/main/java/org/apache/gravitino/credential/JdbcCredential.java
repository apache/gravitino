/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** JDBC credential for accessing JDBC backend services. */
public class JdbcCredential implements Credential {

  /** JDBC credential type. */
  public static final String JDBC_CREDENTIAL_TYPE = "jdbc";
  /** The JDBC user name. */
  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  /** The JDBC password. */
  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";

  private String jdbcUser;
  private String jdbcPassword;

  /**
   * Constructs an instance of {@link JdbcCredential} with the JDBC user and password.
   *
   * @param jdbcUser The JDBC user name.
   * @param jdbcPassword The JDBC password.
   */
  public JdbcCredential(String jdbcUser, String jdbcPassword) {
    validate(jdbcUser, jdbcPassword, 0);
    this.jdbcUser = jdbcUser;
    this.jdbcPassword = jdbcPassword;
  }

  /**
   * This is the constructor that is used by credential factory to create an instance of credential
   * according to the credential information.
   */
  public JdbcCredential() {}

  @Override
  public String credentialType() {
    return JDBC_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return 0;
  }

  @Override
  public Map<String, String> credentialInfo() {
    return (new ImmutableMap.Builder<String, String>())
        .put(GRAVITINO_JDBC_USER, jdbcUser)
        .put(GRAVITINO_JDBC_PASSWORD, jdbcPassword)
        .build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String jdbcUser = credentialInfo.get(GRAVITINO_JDBC_USER);
    String jdbcPassword = credentialInfo.get(GRAVITINO_JDBC_PASSWORD);
    validate(jdbcUser, jdbcPassword, expireTimeInMs);
    this.jdbcUser = jdbcUser;
    this.jdbcPassword = jdbcPassword;
  }

  /**
   * Get JDBC user name.
   *
   * @return The JDBC user name.
   */
  public String jdbcUser() {
    return jdbcUser;
  }

  /**
   * Get JDBC password.
   *
   * @return The JDBC password.
   */
  public String jdbcPassword() {
    return jdbcPassword;
  }

  private void validate(String jdbcUser, String jdbcPassword, long expireTimeInMs) {
    Preconditions.checkArgument(StringUtils.isNotBlank(jdbcUser), "JDBC user should not be empty");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcPassword), "JDBC password should not be empty");
    Preconditions.checkArgument(
        expireTimeInMs == 0, "The expire time of JdbcCredential should be 0");
  }
}
