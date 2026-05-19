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

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Generate JDBC user and password credentials to access JDBC backend services. */
public class JdbcCredentialProvider implements CredentialProvider {

  private String jdbcUser;
  private String jdbcPassword;

  @Override
  public void initialize(Map<String, String> properties) {
    if (properties == null) {
      return;
    }
    // jdbcUser / jdbcPassword will be null when the catalog was not configured with JDBC
    // credentials. getCredential() handles this by returning Optional.empty().
    // Note: The property keys "jdbc-user" and "jdbc-password" match the configuration keys
    // used by JDBC-based catalogs (e.g., Iceberg JDBC backend, Paimon JDBC backend).
    this.jdbcUser = properties.get(JdbcCredential.GRAVITINO_JDBC_USER);
    this.jdbcPassword = properties.get(JdbcCredential.GRAVITINO_JDBC_PASSWORD);
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return JdbcCredential.JDBC_CREDENTIAL_TYPE;
  }

  @Override
  public Optional<Credential> getCredential(CredentialContext context) {
    if (StringUtils.isBlank(jdbcUser) || StringUtils.isBlank(jdbcPassword)) {
      return Optional.empty();
    }
    return Optional.of(new JdbcCredential(jdbcUser, jdbcPassword));
  }
}
