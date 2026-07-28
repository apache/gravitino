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
package org.apache.gravitino.catalog.clickhouse.converter;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeUtils {

  private static final Pattern DATETIME_PATTERN =
      Pattern.compile("^DateTime(?:\\(\\s*'([^']+)'\\s*\\))?$");
  private static final Pattern DATETIME64_PATTERN =
      Pattern.compile("^DateTime64\\(\\s*(\\d+)\\s*(?:,\\s*'([^']+)'\\s*)?\\)$");

  private TypeUtils() {}

  public static String stripNullable(String typeName) {
    return typeName.replaceFirst("^Nullable\\((.*)\\)$", "$1");
  }

  public static String stripLowCardinality(String typeName) {
    return typeName.replaceFirst("^LowCardinality\\((.*)\\)$", "$1");
  }

  public static Integer extractDateTimePrecision(String typeName) {
    Matcher dateTime64Matcher = DATETIME64_PATTERN.matcher(typeName);
    if (dateTime64Matcher.matches()) {
      return Integer.parseInt(dateTime64Matcher.group(1));
    }
    if (DATETIME_PATTERN.matcher(typeName).matches()) {
      return 0;
    }
    return null;
  }

  static Optional<String> extractDateTimeTimeZone(String typeName) {
    Matcher dateTime64Matcher = DATETIME64_PATTERN.matcher(typeName);
    if (dateTime64Matcher.matches()) {
      return Optional.ofNullable(dateTime64Matcher.group(2));
    }
    Matcher dateTimeMatcher = DATETIME_PATTERN.matcher(typeName);
    if (dateTimeMatcher.matches()) {
      return Optional.ofNullable(dateTimeMatcher.group(1));
    }
    return Optional.empty();
  }
}
