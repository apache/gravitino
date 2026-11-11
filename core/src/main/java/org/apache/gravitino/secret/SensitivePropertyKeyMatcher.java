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
 * Matcher for credential-like property key keywords.
 *
 * <p>The active set starts as {@link SensitivePropertyKeyKeywords#defaultKeywords()}. Server
 * configuration replaces that set entirely. Each keyword is matched as a case-insensitive literal
 * substring of the property key.
 */
final class SensitivePropertyKeyMatcher {

  private static final Set<String> DEFAULT_KEYWORDS =
      ImmutableSet.copyOf(SensitivePropertyKeyKeywords.defaultKeywords());

  private static volatile Set<String> keywords = DEFAULT_KEYWORDS;

  private SensitivePropertyKeyMatcher() {}

  static boolean matches(String lowerKey) {
    for (String keyword : keywords) {
      if (lowerKey.contains(keyword)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Replaces the active sensitive key keywords from Gravitino configuration.
   *
   * @param config server configuration
   */
  static void configure(Config config) {
    configure(config.get(Configs.SENSITIVE_KEY_KEYWORDS));
  }

  static void configure(List<String> configuredKeywords) {
    Set<String> normalizedKeywords = new LinkedHashSet<>();
    if (configuredKeywords != null) {
      for (String keyword : configuredKeywords) {
        Preconditions.checkArgument(
            SensitivePropertyKeyKeywords.isValidKeyword(keyword),
            SensitivePropertyKeyKeywords.invalidKeywordMessage());
        normalizedKeywords.add(keyword.trim().toLowerCase(Locale.ROOT));
      }
    }
    keywords = ImmutableSet.copyOf(normalizedKeywords);
  }

  @VisibleForTesting
  static void resetToDefaults() {
    keywords = DEFAULT_KEYWORDS;
  }
}
