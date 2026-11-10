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
package org.apache.gravitino.idp.storage.relational.converters;

import com.google.common.base.Preconditions;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;

/** Factory for built-in IdP JDBC SQL exception converters. */
public class IdpSQLExceptionConverterFactory {
  private static final Pattern TYPE_PATTERN = Pattern.compile("jdbc:(\\w+):");
  private static volatile IdpSQLExceptionConverter converter;

  private IdpSQLExceptionConverterFactory() {}

  /**
   * Initializes the SQL exception converter from the JDBC backend URL in config.
   *
   * @param config The server configuration.
   */
  public static synchronized void initConverter(Config config) {
    if (converter == null) {
      String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
      converter = new IdpSQLExceptionConverter(parseJdbcType(jdbcUrl));
    }
  }

  /**
   * Returns the initialized SQL exception converter.
   *
   * @return The SQL exception converter.
   */
  public static IdpSQLExceptionConverter getConverter() {
    Preconditions.checkState(converter != null, "Exception converter is not initialized.");
    return converter;
  }

  /** Closes and resets the SQL exception converter. */
  public static void close() {
    if (converter != null) {
      synchronized (IdpSQLExceptionConverterFactory.class) {
        if (converter != null) {
          converter = null;
        }
      }
    }
  }

  private static IdpSQLExceptionConverter.JdbcType parseJdbcType(String jdbcUrl) {
    Matcher typeMatcher = TYPE_PATTERN.matcher(jdbcUrl);
    if (!typeMatcher.find()) {
      throw new IllegalArgumentException(
          String.format("Cannot find jdbc type in jdbc url: %s", jdbcUrl));
    }

    String jdbcType = typeMatcher.group(1);
    if (jdbcType.equalsIgnoreCase("mysql")) {
      return IdpSQLExceptionConverter.JdbcType.MYSQL;
    } else if (jdbcType.equalsIgnoreCase("h2")) {
      return IdpSQLExceptionConverter.JdbcType.H2;
    } else if (jdbcType.equalsIgnoreCase("postgresql")) {
      return IdpSQLExceptionConverter.JdbcType.POSTGRESQL;
    }
    throw new IllegalArgumentException(String.format("Unsupported jdbc type: %s", jdbcType));
  }
}
