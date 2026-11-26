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
import org.apache.commons.lang3.StringUtils;

/**
 * Checks the list and map fields of job templates and job template updates, so an invalid element,
 * key or value is rejected with an {@link IllegalArgumentException} naming the field, instead of
 * failing later with a {@link NullPointerException}.
 *
 * <p>Resource URIs and map keys must not be blank. Arguments and map values may be empty, because a
 * placeholder with an empty default value, for example {@code {{older_than:-}}}, resolves to an
 * empty string when the job runs, and the runtime job template is built with the same builders.
 */
final class JobTemplateValidations {

  private JobTemplateValidations() {}

  /**
   * Checks that a list field, such as the arguments, has no null element. Empty elements are
   * allowed.
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
   * Checks that a list field of resource URIs, such as the jars, has no null or blank element.
   *
   * @param field the name of the field, used in the error message
   * @param values the list, may be null
   * @throws IllegalArgumentException if the list has a null or blank element
   */
  static void checkNoBlankElements(String field, @Nullable List<String> values) {
    if (values == null) {
      return;
    }
    for (int i = 0; i < values.size(); i++) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(values.get(i)),
          "\"%s\" must not contain null or blank elements, found one at index %s",
          field,
          i);
    }
  }

  /**
   * Checks that a map field has no null or blank key and no null value. Empty values are allowed.
   *
   * @param field the name of the field, used in the error message
   * @param entries the map, may be null
   * @throws IllegalArgumentException if the map has a null or blank key, or a null value
   */
  static void checkNoBlankKeysOrNullValues(String field, @Nullable Map<String, String> entries) {
    if (entries == null) {
      return;
    }
    entries.forEach(
        (key, value) -> {
          Preconditions.checkArgument(
              StringUtils.isNotBlank(key), "\"%s\" must not contain null or blank keys", field);
          Preconditions.checkArgument(
              value != null,
              "\"%s\" must not contain null values, found one for key \"%s\"",
              field,
              key);
        });
  }

  /**
   * Returns an immutable copy of a list field, or an empty list if the field is null. Check the
   * elements first, the copy rejects null elements with a {@link NullPointerException}.
   *
   * @param values the list, may be null
   * @return the immutable copy
   */
  static List<String> immutableListCopy(@Nullable List<String> values) {
    return values == null ? ImmutableList.of() : ImmutableList.copyOf(values);
  }

  /**
   * Returns an immutable copy of a map field, or an empty map if the field is null. Check the
   * entries first, the copy rejects null keys and values with a {@link NullPointerException}.
   *
   * @param entries the map, may be null
   * @return the immutable copy
   */
  static Map<String, String> immutableMapCopy(@Nullable Map<String, String> entries) {
    return entries == null ? ImmutableMap.of() : ImmutableMap.copyOf(entries);
  }
}
