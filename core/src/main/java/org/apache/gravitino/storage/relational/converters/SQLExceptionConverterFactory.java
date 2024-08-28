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
package org.apache.gravitino.storage.relational.converters;

import com.google.common.base.Preconditions;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;

public class SQLExceptionConverterFactory {
  private static final Pattern TYPE_PATTERN = Pattern.compile("jdbc:(\\w+):");
  private static SQLExceptionConverter converter;

  private SQLExceptionConverterFactory() {}

  public static synchronized void initConverter(Config config) {
    if (converter == null) {
      String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
      Matcher typeMatcher = TYPE_PATTERN.matcher(jdbcUrl);
      if (typeMatcher.find()) {
        String jdbcType = typeMatcher.group(1);
        if (jdbcType.equalsIgnoreCase("mysql")) {
          converter = new MySQLExceptionConverter();
        } else if (jdbcType.equalsIgnoreCase("h2")) {
          converter = new H2ExceptionConverter();
        } else if (jdbcType.equalsIgnoreCase("postgresql")) {
          converter = new PostgreSQLExceptionConverter();
        } else {
          throw new IllegalArgumentException(String.format("Unsupported jdbc type: %s", jdbcType));
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Cannot find jdbc type in jdbc url: %s", jdbcUrl));
      }
    }
  }

  public static SQLExceptionConverter getConverter() {
    Preconditions.checkState(converter != null, "Exception converter is not initialized.");
    return converter;
  }
}
