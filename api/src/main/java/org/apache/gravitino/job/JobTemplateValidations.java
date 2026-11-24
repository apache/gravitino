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

package org.apache.gravitino.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Checks the list and map fields of job templates and job template updates, so a null element, key
 * or value is rejected with an {@link IllegalArgumentException} naming the field, instead of
 * failing later with a {@link NullPointerException}.
 */
final class JobTemplateValidations {

  private JobTemplateValidations() {}

  /**
   * Checks that a list field has no null element.
   *
   * @param field the name of the field, used in the error message
   * @param values the list, may be null
   * @throws IllegalArgumentException if the list has a null element
   */
  static void checkNoNullElements(String field, @Nullable List<String> values) {
    if (values == null) {
      return;
    }
    for (int i = 0; i < values.size(); i++) {
      Preconditions.checkArgument(
          values.get(i) != null,
          "\"%s\" must not contain null elements, found one at index %s",
          field,
          i);
    }
  }

  /**
   * Checks that a map field has no null key or value.
   *
   * @param field the name of the field, used in the error message
   * @param entries the map, may be null
   * @throws IllegalArgumentException if the map has a null key or a null value
   */
  static void checkNoNullEntries(String field, @Nullable Map<String, String> entries) {
    if (entries == null) {
      return;
    }
    entries.forEach(
        (key, value) -> {
          Preconditions.checkArgument(key != null, "\"%s\" must not contain null keys", field);
          Preconditions.checkArgument(
              value != null,
              "\"%s\" must not contain null values, found one for key \"%s\"",
              field,
              key);
        });
  }

  /**
   * Returns an immutable copy of a list field, or an empty list if the field is null.
   *
   * @param field the name of the field, used in the error message
   * @param values the list, may be null
   * @return the immutable copy
   * @throws IllegalArgumentException if the list has a null element
   */
  static List<String> immutableListCopy(String field, @Nullable List<String> values) {
    checkNoNullElements(field, values);
    return values == null ? ImmutableList.of() : ImmutableList.copyOf(values);
  }

  /**
   * Returns an immutable copy of a map field, or an empty map if the field is null.
   *
   * @param field the name of the field, used in the error message
   * @param entries the map, may be null
   * @return the immutable copy
   * @throws IllegalArgumentException if the map has a null key or a null value
   */
  static Map<String, String> immutableMapCopy(String field, @Nullable Map<String, String> entries) {
    checkNoNullEntries(field, entries);
    return entries == null ? ImmutableMap.of() : ImmutableMap.copyOf(entries);
  }
}
