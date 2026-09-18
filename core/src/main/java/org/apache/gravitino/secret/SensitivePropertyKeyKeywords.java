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

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Built-in credential-like substrings used for sensitive property key detection. */
public final class SensitivePropertyKeyKeywords {

  static final Set<String> BUILTIN_SUBSTRINGS =
      ImmutableSet.of("secret", "password", "token", "credential", "access", "account");

  static final Pattern BUILTIN_PATTERN =
      Pattern.compile(".*(" + BUILTIN_SUBSTRINGS.stream().collect(Collectors.joining("|")) + ").*");

  private static final String INVALID_ADDITIONAL_KEYWORD_MSG =
      "Additional keywords must be non-blank and must not contain a built-in sensitive keyword: "
          + String.join(", ", BUILTIN_SUBSTRINGS);

  private SensitivePropertyKeyKeywords() {}

  /**
   * Returns whether {@code keyword} may be configured as an additional sensitive key substring.
   *
   * <p>Built-in keywords are always matched. An additional keyword that contains one of them adds
   * no new matches and is rejected. Any other non-blank substring is allowed, including common
   * credential typos (for example {@code passwrod}) and extra credential-like words (for example
   * {@code private}).
   *
   * @param keyword candidate additional keyword
   * @return true when the keyword may be configured
   */
  public static boolean isValidAdditionalKeyword(String keyword) {
    if (StringUtils.isBlank(keyword)) {
      return false;
    }
    String normalized = keyword.trim().toLowerCase(Locale.ROOT);
    return BUILTIN_SUBSTRINGS.stream().noneMatch(normalized::contains);
  }

  /**
   * Returns the validation message for an illegal additional sensitive keyword.
   *
   * @return the validation message
   */
  public static String invalidAdditionalKeywordMessage() {
    return INVALID_ADDITIONAL_KEYWORD_MSG;
  }
}
