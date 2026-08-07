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
package org.apache.gravitino.rel.metric;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

final class SemanticModelUtils {

  private SemanticModelUtils() {}

  static String requireNonBlank(String value, String fieldName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value), "%s must not be null or blank", fieldName);
    return value;
  }

  static <T> List<T> immutableList(List<T> values, String fieldName) {
    if (values == null) {
      return Collections.emptyList();
    }
    Preconditions.checkArgument(
        values.stream().noneMatch(value -> value == null), "%s must not contain null", fieldName);
    return Collections.unmodifiableList(new ArrayList<>(values));
  }

  static <T> List<T> requireNonEmptyList(List<T> values, String fieldName) {
    Preconditions.checkArgument(
        values != null && !values.isEmpty(), "%s must not be null or empty", fieldName);
    return immutableList(values, fieldName);
  }

  static List<List<String>> immutableNestedStringList(List<List<String>> values, String fieldName) {
    if (values == null) {
      return Collections.emptyList();
    }
    List<List<String>> copied = new ArrayList<>(values.size());
    for (List<String> value : values) {
      copied.add(requireNonEmptyList(value, fieldName + " entry"));
    }
    return Collections.unmodifiableList(copied);
  }

  static <K, V> Map<K, V> immutableMap(Map<K, V> values) {
    if (values == null) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new java.util.LinkedHashMap<>(values));
  }
}
