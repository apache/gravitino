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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Regex-based group mapper that extracts group names using regex patterns with capturing groups.
 *
 * <p>This implementation is thread-safe as Pattern.matcher() creates thread-local Matcher
 * instances.
 */
public class RegexGroupMapper implements GroupMapper {

  private static final Logger LOG = LoggerFactory.getLogger(RegexGroupMapper.class);

  private final Pattern pattern;

  /**
   * Creates a new regex group mapper.
   *
   * @param patternStr the regex pattern with a capturing group (required)
   * @throws IllegalArgumentException if the pattern string has invalid regex syntax
   */
  public RegexGroupMapper(String patternStr) {
    if (patternStr == null || patternStr.isEmpty()) {
      throw new IllegalArgumentException("Pattern string cannot be null or empty");
    }
    this.pattern = Pattern.compile(patternStr);
    LOG.info("Initialized RegexGroupMapper with pattern: {}", patternStr);
  }

  /**
   * Maps a list of group strings to a new list of group strings using the configured regex pattern.
   *
   * @param groups the list of group strings to map
   * @return a list of mapped group strings
   */
  @Override
  public List<String> map(List<String> groups) {
    if (groups == null || groups.isEmpty()) {
      return new ArrayList<>();
    }

    List<String> mappedGroups = new ArrayList<>();
    for (String group : groups) {
      if (group == null) {
        continue;
      }
      try {
        Matcher matcher = pattern.matcher(group);
        if (matcher.find() && matcher.groupCount() >= 1) {
          String extracted = matcher.group(1);
          if (extracted != null && !extracted.isEmpty()) {
            mappedGroups.add(extracted);
          } else {
            mappedGroups.add(group);
          }
        } else {
          mappedGroups.add(group);
        }
      } catch (Exception e) {
        String message =
            String.format(
                "Error applying regex pattern '%s' to group '%s'", pattern.pattern(), group);
        LOG.error("{}: {}", message, e.getMessage());
        throw new IllegalArgumentException(message, e);
      }
    }
    return mappedGroups;
  }
}
