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
package org.apache.gravitino.secret;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Matches credential-like property keys for masking and {@code getSecrets} recovery.
 *
 * <p>Besides the built-in substring keywords ({@code secret}, {@code password}, {@code token},
 * {@code credential}, {@code access}, {@code account}), Gravitino can be configured with extra typo
 * patterns and optional fuzzy matching so mistyped credential property names (for example {@code
 * jdbc-passwrod}) are still treated as sensitive.
 */
final class SensitivePropertyKeyMatcher {

  private static final Pattern BUILTIN_SENSITIVE_KEY_PATTERN =
      Pattern.compile(".*(secret|password|token|credential|access|account).*");

  private static final Pattern TOKEN_SPLIT_PATTERN = Pattern.compile("[-_.]+");

  /** Canonical keywords used by the built-in pattern and fuzzy matching. */
  static final List<String> CANONICAL_SENSITIVE_KEYWORDS =
      ImmutableList.of("secret", "password", "token", "credential", "access", "account");

  /** Minimum token length considered for fuzzy matching. */
  private static final int MIN_FUZZY_TOKEN_LENGTH = 5;

  private static volatile MatcherState state = MatcherState.defaults();

  private SensitivePropertyKeyMatcher() {}

  static boolean isSensitive(@Nullable String key) {
    if (key == null || key.isEmpty()) {
      return false;
    }
    String lowerKey = key.toLowerCase(Locale.ROOT);
    if (BUILTIN_SENSITIVE_KEY_PATTERN.matcher(lowerKey).matches()) {
      return true;
    }

    MatcherState current = state;
    for (String typoPattern : current.typoPatterns) {
      if (lowerKey.contains(typoPattern)) {
        return true;
      }
    }

    if (current.fuzzyMaxDistance <= 0) {
      return false;
    }

    String[] tokens = TOKEN_SPLIT_PATTERN.split(lowerKey);
    for (String token : tokens) {
      if (token.length() < MIN_FUZZY_TOKEN_LENGTH) {
        continue;
      }
      for (String reference : current.fuzzyReferences) {
        if (damerauLevenshteinDistance(token, reference) <= current.fuzzyMaxDistance) {
          return true;
        }
      }
    }
    return false;
  }

  static void configure(List<String> typoPatterns, int fuzzyMaxDistance) {
    state = MatcherState.from(typoPatterns, fuzzyMaxDistance);
  }

  @VisibleForTesting
  static void resetToDefaults() {
    state = MatcherState.defaults();
  }

  private static int damerauLevenshteinDistance(String left, String right) {
    int leftLength = left.length();
    int rightLength = right.length();
    if (leftLength == 0) {
      return rightLength;
    }
    if (rightLength == 0) {
      return leftLength;
    }

    int[][] distance = new int[leftLength + 1][rightLength + 1];
    for (int i = 0; i <= leftLength; i++) {
      distance[i][0] = i;
    }
    for (int j = 0; j <= rightLength; j++) {
      distance[0][j] = j;
    }

    for (int i = 1; i <= leftLength; i++) {
      for (int j = 1; j <= rightLength; j++) {
        int substitutionCost = left.charAt(i - 1) == right.charAt(j - 1) ? 0 : 1;
        distance[i][j] =
            Math.min(
                Math.min(distance[i - 1][j] + 1, distance[i][j - 1] + 1),
                distance[i - 1][j - 1] + substitutionCost);

        if (i > 1
            && j > 1
            && left.charAt(i - 1) == right.charAt(j - 2)
            && left.charAt(i - 2) == right.charAt(j - 1)) {
          distance[i][j] = Math.min(distance[i][j], distance[i - 2][j - 2] + 1);
        }
      }
    }
    return distance[leftLength][rightLength];
  }

  private static final class MatcherState {
    private final Set<String> typoPatterns;
    private final Set<String> fuzzyReferences;
    private final int fuzzyMaxDistance;

    private MatcherState(
        Set<String> typoPatterns, Set<String> fuzzyReferences, int fuzzyMaxDistance) {
      this.typoPatterns = typoPatterns;
      this.fuzzyReferences = fuzzyReferences;
      this.fuzzyMaxDistance = fuzzyMaxDistance;
    }

    private static MatcherState defaults() {
      return from(List.of(), 1);
    }

    private static MatcherState from(List<String> typoPatterns, int fuzzyMaxDistance) {
      Set<String> normalizedTypoPatterns = new LinkedHashSet<>();
      if (typoPatterns != null) {
        for (String typoPattern : typoPatterns) {
          if (StringUtils.isNotBlank(typoPattern)) {
            normalizedTypoPatterns.add(typoPattern.trim().toLowerCase(Locale.ROOT));
          }
        }
      }

      Set<String> fuzzyReferences = new LinkedHashSet<>(CANONICAL_SENSITIVE_KEYWORDS);
      fuzzyReferences.addAll(normalizedTypoPatterns);

      return new MatcherState(
          ImmutableSet.copyOf(normalizedTypoPatterns),
          ImmutableSet.copyOf(fuzzyReferences),
          Math.max(0, fuzzyMaxDistance));
    }
  }
}
