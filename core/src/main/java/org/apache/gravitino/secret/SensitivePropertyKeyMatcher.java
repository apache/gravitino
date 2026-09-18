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
 * class adds optional configured keywords beyond the built-in set, for example common typos ({@code
 * passwrod}) or extra credential-like words ({@code private}). Each keyword is matched as a
 * case-insensitive substring of the property key.
 */
final class SensitivePropertyKeyMatcher {

  private static volatile Set<String> additionalKeywords = Set.of();

  private SensitivePropertyKeyMatcher() {}

  static boolean matchesAdditionalKeyword(String lowerKey) {
    for (String additionalKeyword : additionalKeywords) {
      if (lowerKey.contains(additionalKeyword)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Applies additional sensitive key keyword settings from Gravitino configuration.
   *
   * @param config server configuration
   */
  static void configure(Config config) {
    configure(config.get(Configs.SENSITIVE_KEY_ADDITIONAL_KEYWORDS));
  }

  static void configure(List<String> configuredAdditionalKeywords) {
    Set<String> normalizedKeywords = new LinkedHashSet<>();
    if (configuredAdditionalKeywords != null) {
      for (String additionalKeyword : configuredAdditionalKeywords) {
        Preconditions.checkArgument(
            SensitivePropertyKeyKeywords.isValidAdditionalKeyword(additionalKeyword),
            SensitivePropertyKeyKeywords.invalidAdditionalKeywordMessage());
        normalizedKeywords.add(additionalKeyword.trim().toLowerCase(Locale.ROOT));
      }
    }
    additionalKeywords = ImmutableSet.copyOf(normalizedKeywords);
  }

  @VisibleForTesting
  static void resetToDefaults() {
    additionalKeywords = Set.of();
  }
}
