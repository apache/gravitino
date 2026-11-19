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

package org.apache.gravitino.kb;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;

final class ArticleValidation {
  private ArticleValidation() {}

  static String title(String value) {
    Preconditions.checkArgument(
        value != null && !value.trim().isEmpty(), "Article title must not be blank");
    return length(value.trim(), KnowledgeBaseArticle.TITLE_MAX_LENGTH, "Title");
  }

  static String length(@Nullable String value, int maximum, String field) {
    Preconditions.checkArgument(
        value == null || value.codePointCount(0, value.length()) <= maximum,
        "%s exceeds %s characters",
        field,
        maximum);
    return value;
  }

  static List<String> synonyms(List<String> values) {
    Set<String> seen = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    ImmutableList.Builder<String> result = ImmutableList.builder();
    for (String value : values) {
      String trimmed = length(value.trim(), KnowledgeBaseArticle.SYNONYM_MAX_LENGTH, "Synonym");
      if (seen.add(trimmed)) {
        result.add(trimmed);
      }
    }
    Preconditions.checkArgument(
        seen.size() <= KnowledgeBaseArticle.SYNONYMS_MAX_COUNT, "Too many synonyms");
    return result.build();
  }
}
