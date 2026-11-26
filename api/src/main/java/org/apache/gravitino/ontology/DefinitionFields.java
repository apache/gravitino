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
package org.apache.gravitino.ontology;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Copies JSON-compatible values without retaining mutable caller data. */
final class DefinitionFields {

  private DefinitionFields() {}

  static Map<String, Object> copy(Map<?, ?> source) {
    Map<String, Object> target = new LinkedHashMap<>();
    source.forEach(
        (key, value) -> {
          if (!(key instanceof String)) {
            throw new IllegalArgumentException("Definition keys must be strings");
          }
          target.put((String) key, copyValue(value));
        });
    return Collections.unmodifiableMap(target);
  }

  @Nullable
  private static Object copyValue(@Nullable Object value) {
    if (value instanceof Map) {
      return copy((Map<?, ?>) value);
    }
    if (value instanceof List) {
      List<Object> copy = new ArrayList<>();
      ((List<?>) value).forEach(item -> copy.add(copyValue(item)));
      return Collections.unmodifiableList(copy);
    }
    if (value == null
        || value instanceof String
        || value instanceof Boolean
        || value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof BigInteger
        || value instanceof BigDecimal
        || (value instanceof Double && Double.isFinite((Double) value))
        || (value instanceof Float && Float.isFinite((Float) value))) {
      return value;
    }
    throw new IllegalArgumentException("Definition values must be representable as JSON");
  }
}
