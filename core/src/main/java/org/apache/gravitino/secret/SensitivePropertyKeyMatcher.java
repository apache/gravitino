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
 * substrings so mistyped credential property names (for example {@code jdbc-passwrod}) are still
 * treated as sensitive.
 */
final class SensitivePropertyKeyMatcher {

  private static final Pattern BUILTIN_SENSITIVE_KEY_PATTERN =
      Pattern.compile(".*(secret|password|token|credential|access|account).*");

  private static volatile Set<String> typoPatterns = Set.of();

  private SensitivePropertyKeyMatcher() {}

  static boolean isSensitive(@Nullable String key) {
    if (key == null || key.isEmpty()) {
      return false;
    }
    String lowerKey = key.toLowerCase(Locale.ROOT);
    if (BUILTIN_SENSITIVE_KEY_PATTERN.matcher(lowerKey).matches()) {
      return true;
    }
    for (String typoPattern : typoPatterns) {
      if (lowerKey.contains(typoPattern)) {
        return true;
      }
    }
    return false;
  }

  static void configure(List<String> configuredTypoPatterns) {
    Set<String> normalizedTypoPatterns = new LinkedHashSet<>();
    if (configuredTypoPatterns != null) {
      for (String typoPattern : configuredTypoPatterns) {
        if (StringUtils.isNotBlank(typoPattern)) {
          normalizedTypoPatterns.add(typoPattern.trim().toLowerCase(Locale.ROOT));
        }
      }
    }
    typoPatterns = ImmutableSet.copyOf(normalizedTypoPatterns);
  }

  @VisibleForTesting
  static void resetToDefaults() {
    typoPatterns = Set.of();
  }
}
