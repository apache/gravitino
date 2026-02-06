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

package org.apache.gravitino.auth;

import java.security.Principal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.UserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regex-based principal mapper that extracts username using regex patterns with capturing groups.
 *
 * <p>This implementation is thread-safe as Pattern.matcher() creates thread-local Matcher
 * instances.
 */
public class RegexPrincipalMapper implements PrincipalMapper {

  private static final Logger LOG = LoggerFactory.getLogger(RegexPrincipalMapper.class);

  private final Pattern pattern;

  /**
   * Creates a new regex principal mapper.
   *
   * @param patternStr the regex pattern with a capturing group (required)
   * @throws IllegalArgumentException if the pattern string has invalid regex syntax
   */
  public RegexPrincipalMapper(String patternStr) {
    if (patternStr == null || patternStr.isEmpty()) {
      throw new IllegalArgumentException("Pattern string cannot be null or empty");
    }
    this.pattern = Pattern.compile(patternStr);
    LOG.info("Initialized RegexPrincipalMapper with pattern: {}", patternStr);
  }

  /**
   * Maps a principal string to a Principal using the configured regex pattern.
   *
   * @param principal the principal string to map
   * @return a Principal containing the extracted username from the first capturing group, or the
   *     original principal if no match
   * @throws IllegalArgumentException if pattern matching fails
   */
  @Override
  public Principal map(String principal) {
    if (principal == null) {
      return null;
    }

    try {
      Matcher matcher = pattern.matcher(principal);

      // If pattern matches and has at least one capturing group, return the first group
      if (matcher.find() && matcher.groupCount() >= 1) {
        String extracted = matcher.group(1);
        // Return extracted value if it's not null and not empty, otherwise original principal
        String username = (extracted != null && !extracted.isEmpty()) ? extracted : principal;
        return new UserPrincipal(username);
      }

      // If pattern doesn't match or has no capturing groups, return original principal
      return new UserPrincipal(principal);

    } catch (Exception e) {
      String message =
          String.format(
              "Error applying regex pattern '%s' to principal '%s'", pattern.pattern(), principal);
      LOG.error("{}: {}", message, e.getMessage());
      throw new IllegalArgumentException(message, e);
    }
  }

  /**
   * Gets the configured regex pattern string.
   *
   * @return the regex pattern string
   */
  public String getPatternString() {
    return pattern.pattern();
  }
}
