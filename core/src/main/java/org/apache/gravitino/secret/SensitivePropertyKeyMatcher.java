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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;

/**
 * Supplementary matcher for credential-like property key typos.
 *
 * <p>The built-in sensitive key pattern in {@link SecretPropertyUtils} always applies first. This
 * class adds optional configured typo substrings so mistyped credential property names (for example
 * {@code jdbc-passwrod}) are still treated as sensitive.
 */
final class SensitivePropertyKeyMatcher {

  private static volatile Set<String> typoPatterns = Set.of();

  private SensitivePropertyKeyMatcher() {}

  static boolean matchesTypoPattern(String lowerKey) {
    for (String typoPattern : typoPatterns) {
      if (lowerKey.contains(typoPattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Applies typo pattern settings from Gravitino configuration.
   *
   * @param config server configuration
   */
  static void configure(Config config) {
    configure(config.get(Configs.SENSITIVE_PROPERTY_KEY_TYPO_PATTERNS));
  }

  static void configure(List<String> configuredTypoPatterns) {
    Set<String> normalizedTypoPatterns = new LinkedHashSet<>();
    if (configuredTypoPatterns != null) {
      for (String typoPattern : configuredTypoPatterns) {
        Preconditions.checkArgument(
            SensitivePropertyKeyKeywords.isValidTypoPattern(typoPattern),
            SensitivePropertyKeyKeywords.invalidTypoPatternMessage());
        normalizedTypoPatterns.add(typoPattern.trim().toLowerCase(Locale.ROOT));
      }
    }
    typoPatterns = ImmutableSet.copyOf(normalizedTypoPatterns);
  }

  @VisibleForTesting
  static void resetToDefaults() {
    typoPatterns = Set.of();
  }
}
