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

import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Default credential-like substrings used for sensitive property key detection.
 *
 * <p>These keywords are the default value of {@link
 * org.apache.gravitino.Configs#SENSITIVE_KEY_KEYWORDS}. A configured list replaces them entirely.
 */
public final class SensitivePropertyKeyKeywords {

  static final List<String> DEFAULT_KEYWORDS =
      List.of("secret", "password", "token", "credential", "access", "account");

  private static final String INVALID_KEYWORD_MSG = "Sensitive key keywords must be non-blank";

  private SensitivePropertyKeyKeywords() {}

  /**
   * Returns the default sensitive property key keywords.
   *
   * @return an immutable list of default keywords
   */
  public static List<String> defaultKeywords() {
    return DEFAULT_KEYWORDS;
  }

  /**
   * Returns whether {@code keyword} may appear in {@link
   * org.apache.gravitino.Configs#SENSITIVE_KEY_KEYWORDS}.
   *
   * <p>Blank entries are rejected. The configured list replaces the defaults, so a deployment can
   * omit a default keyword such as {@code access} or add one such as {@code private}.
   *
   * @param keyword candidate keyword
   * @return true when the keyword may be configured
   */
  public static boolean isValidKeyword(String keyword) {
    return StringUtils.isNotBlank(keyword);
  }

  /**
   * Returns the validation message for an illegal sensitive key keyword.
   *
   * @return the validation message
   */
  public static String invalidKeywordMessage() {
    return INVALID_KEYWORD_MSG;
  }
}
