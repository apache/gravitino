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
 * Supplementary matcher for credential-like property key substrings configured at runtime.
 *
 * <p>The built-in sensitive key pattern in {@link SecretPropertyUtils} always applies first. This
 * class adds optional configured substrings beyond the built-in keywords, for example common typos
 * ({@code passwrod}) or extra credential-like words ({@code private}).
 */
final class SensitivePropertyKeyMatcher {

  private static volatile Set<String> additionalPatterns = Set.of();

  private SensitivePropertyKeyMatcher() {}

  static boolean matchesAdditionalPattern(String lowerKey) {
    for (String additionalPattern : additionalPatterns) {
      if (lowerKey.contains(additionalPattern)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Applies additional sensitive key substring settings from Gravitino configuration.
   *
   * @param config server configuration
   */
  static void configure(Config config) {
    configure(config.get(Configs.SENSITIVE_PROPERTY_KEY_ADDITIONAL_PATTERNS));
  }

  static void configure(List<String> configuredAdditionalPatterns) {
    Set<String> normalizedPatterns = new LinkedHashSet<>();
    if (configuredAdditionalPatterns != null) {
      for (String additionalPattern : configuredAdditionalPatterns) {
        Preconditions.checkArgument(
            SensitivePropertyKeyKeywords.isValidAdditionalPattern(additionalPattern),
            SensitivePropertyKeyKeywords.invalidAdditionalPatternMessage());
        normalizedPatterns.add(additionalPattern.trim().toLowerCase(Locale.ROOT));
      }
    }
    additionalPatterns = ImmutableSet.copyOf(normalizedPatterns);
  }

  @VisibleForTesting
  static void resetToDefaults() {
    additionalPatterns = Set.of();
  }
}
