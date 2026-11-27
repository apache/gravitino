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
package org.apache.gravitino.paimon.common;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.paimon.options.Options;

/** Translates service and Gravitino catalog properties to native Paimon options. */
public final class PaimonCatalogOptions {

  private static final Map<String, String> ALIASES =
      ImmutableMap.<String, String>builder()
          .put("catalog-backend", "metastore")
          .put("jdbc-user", "jdbc.user")
          .put("jdbc-password", "jdbc.password")
          .put("s3-access-key-id", "s3.access-key")
          .put("s3-secret-access-key", "s3.secret-key")
          .put("s3-session-token", "s3.session-token")
          .put("s3.session.token", "s3.session-token")
          .put("s3a.session-token", "s3.session-token")
          .put("s3a.session.token", "s3.session-token")
          .put("s3-endpoint", "s3.endpoint")
          .put("oss-access-key-id", "fs.oss.accessKeyId")
          .put("oss-secret-access-key", "fs.oss.accessKeySecret")
          .put("oss-security-token", "fs.oss.securityToken")
          .put("oss-endpoint", "fs.oss.endpoint")
          .put("fs.s3a.access-key", "s3.access-key")
          .put("fs.s3a.secret-key", "s3.secret-key")
          .put("fs.s3a.access.key", "s3.access-key")
          .put("fs.s3a.secret.key", "s3.secret-key")
          .put("fs.s3a.session.token", "s3.session-token")
          .put("fs.s3a.session-token", "s3.session-token")
          .put("fs.s3a.endpoint", "s3.endpoint")
          .put("fs.s3a.path.style.access", "s3.path.style.access")
          .build();

  private PaimonCatalogOptions() {}

  /**
   * Converts Gravitino property names and loads an explicitly configured JDBC driver.
   *
   * @param properties the catalog properties
   * @return native Paimon options
   */
  public static Options fromProperties(Map<String, String> properties) {
    Map<String, String> options = new HashMap<>(properties);
    // OSS FileIO accepts case-insensitive spellings of this credential key.
    for (String key : properties.keySet()) {
      if (key.equalsIgnoreCase("fs.oss.securityToken")) {
        translateAlias(options, key, "fs.oss.securityToken");
      }
      if (key.equalsIgnoreCase("client.fs.oss.securityToken")) {
        translateAlias(options, key, "client.fs.oss.securityToken");
      }
    }
    ALIASES.forEach(
        (alias, nativeKey) -> {
          translateAlias(options, alias, nativeKey);
          translateAlias(options, "client." + alias, "client." + nativeKey);
        });
    String driver = options.remove("jdbc-driver");
    if (driver != null) {
      try {
        Class.forName(driver);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Could not load JDBC driver " + driver, e);
      }
    }
    return Options.fromMap(options);
  }

  private static void translateAlias(Map<String, String> options, String alias, String nativeKey) {
    String value = options.remove(alias);
    if (value != null) {
      String previous = options.putIfAbsent(nativeKey, value);
      if (previous != null && !previous.equals(value)) {
        throw new IllegalArgumentException(
            "Conflicting catalog options: " + alias + " and " + nativeKey);
      }
    }
  }
}
