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

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableList;
import java.text.Normalizer;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

final class ArticleValidation {
  private static final Pattern NAME = Pattern.compile("[\\p{L}\\p{Nd}\\p{M}_-]+");

  @Nullable
  static String name(@Nullable String value) {
    if (value != null) {
      value = Normalizer.normalize(value, Normalizer.Form.NFC);
      length(value, KnowledgeBaseArticle.NAME_MAX_LENGTH, "Name");
      Preconditions.checkArgument(
          NAME.matcher(value).matches() && value.codePoints().anyMatch(Character::isLetterOrDigit),
          "Invalid article name: %s",
          value);
    }
    return value;
  }

  private ArticleValidation() {}

  static void checkMetadataText(@Nullable String value, String field) {
    if (value != null) {
      Preconditions.checkArgument(value.indexOf('\0') < 0, "%s must not contain U+0000", field);
      Utf8.encodedLength(value);
    }
  }

  static String title(String value) {
    String title = value == null ? "" : CharMatcher.whitespace().trimFrom(value);
    Preconditions.checkArgument(!title.isEmpty(), "Article title must not be blank");
    return length(title, KnowledgeBaseArticle.TITLE_MAX_LENGTH, "Title");
  }

  static String length(@Nullable String value, int maximum, String field) {
    Preconditions.checkArgument(
        value == null || value.codePointCount(0, value.length()) <= maximum,
        "%s exceeds %s characters",
        field,
        maximum);
    return value;
  }

  static ImmutableList<String> termNames(List<String> values) {
    for (String name : values) {
      Preconditions.checkArgument(
          name != null && !name.trim().isEmpty(), "Term name must not be blank");
    }
    return ImmutableList.copyOf(values);
  }

  static List<String> domainNames(List<String> values) {
    for (String value : values) {
      Preconditions.checkArgument(
          value != null && !value.trim().isEmpty(), "Domain name must not be null or blank");
    }
    return ImmutableList.copyOf(values);
  }

  static void checkProperty(String key, String value) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(key), "Property name must not be null or blank");
    Preconditions.checkArgument(value != null, "Property value must not be null");
    Utf8.encodedLength(key);
    Utf8.encodedLength(value);
  }

  static List<String> synonymsForWrite(List<String> values) {
    List<String> normalized = synonyms(values);
    normalized.forEach(Utf8::encodedLength);
    return normalized;
  }

  static List<String> synonyms(List<String> values) {
    Set<String> seen = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    ImmutableList.Builder<String> result = ImmutableList.builder();
    for (String value : values) {
      Preconditions.checkArgument(value != null, "Synonym must not be null");
      String trimmed = value.trim();
      Preconditions.checkArgument(!trimmed.isEmpty(), "Synonym must not be blank");
      trimmed = length(trimmed, KnowledgeBaseArticle.SYNONYM_MAX_LENGTH, "Synonym");
      if (seen.add(trimmed)) {
        result.add(trimmed);
      }
    }
    Preconditions.checkArgument(
        seen.size() <= KnowledgeBaseArticle.SYNONYMS_MAX_COUNT, "Too many synonyms");
    return result.build();
  }
}
