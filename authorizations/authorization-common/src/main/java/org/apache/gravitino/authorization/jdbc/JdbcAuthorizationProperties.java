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
package org.apache.gravitino.authorization.jdbc;

import java.util.Map;
import org.apache.gravitino.authorization.common.AuthorizationProperties;

/** The properties for JDBC authorization plugin. */
public class JdbcAuthorizationProperties extends AuthorizationProperties {
  private static final String CONFIG_PREFIX = "authorization.jdbc.";
  public static final String JDBC_PASSWORD = CONFIG_PREFIX + "password";
  public static final String JDBC_USERNAME = CONFIG_PREFIX + "username";
  public static final String JDBC_URL = CONFIG_PREFIX + "url";
  public static final String JDBC_DRIVER = CONFIG_PREFIX + "driver";

  public JdbcAuthorizationProperties(Map<String, String> properties) {
    super(properties);
  }

  private void check(String key, String errorMsg) {
    if (!properties.containsKey(key)) {
      throw new IllegalArgumentException(String.format(errorMsg, key));
    }
  }

  @Override
  public String getPropertiesPrefix() {
    return CONFIG_PREFIX;
  }

  @Override
  public void validate() {
    String errorMsg = "%s is required";
    check(JDBC_URL, errorMsg);
    check(JDBC_USERNAME, errorMsg);
    check(JDBC_PASSWORD, errorMsg);
    check(JDBC_DRIVER, errorMsg);
  }
}
