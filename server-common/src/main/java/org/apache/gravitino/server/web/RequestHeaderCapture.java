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

package org.apache.gravitino.server.web;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.audit.AuditLogRedactor;

/** Validates a request-header allowlist and captures bounded values for logs and audit events. */
public final class RequestHeaderCapture {

  static final int MAX_HEADERS = 32;
  static final int MAX_NAME_LENGTH = 128;
  static final int MAX_VALUE_LENGTH = 256;
  static final String TRUNCATED_SUFFIX = "...(truncated)";

  private static final Pattern HEADER_NAME = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");
  private static final Set<String> SENSITIVE_HEADERS =
      ImmutableSet.of(
          "authorization",
          "proxy-authorization",
          "cookie",
          "set-cookie",
          "www-authenticate",
          "proxy-authenticate");

  private final List<String> headerNames;

  /**
   * Creates a capture policy, normalizing names to lowercase and removing duplicates.
   *
   * @param names explicit HTTP header names; an empty list disables capture
   * @throws IllegalArgumentException if a name is invalid, sensitive, or a configured limit is
   *     exceeded
   */
  public RequestHeaderCapture(List<String> names) {
    Set<String> normalized = new LinkedHashSet<>();
    for (String entry : names) {
      String name = entry.trim().toLowerCase(Locale.ROOT);
      Preconditions.checkArgument(
          name.length() <= MAX_NAME_LENGTH && HEADER_NAME.matcher(name).matches(),
          "Invalid request header name in allowlist: %s",
          entry);
      Preconditions.checkArgument(
          !SENSITIVE_HEADERS.contains(name)
              && !AuditLogRedactor.REDACTED_VALUE.equals(AuditLogRedactor.redactValue(name, "")),
          "Sensitive request header cannot be captured: %s",
          entry);
      normalized.add(name);
    }
    Preconditions.checkArgument(
        normalized.size() <= MAX_HEADERS,
        "Request header allowlist must contain at most %s distinct names",
        MAX_HEADERS);
    this.headerNames = ImmutableList.copyOf(normalized);
  }

  /**
   * Captures the first value of each present allowlisted header. Values have control characters
   * replaced with spaces and are truncated to a fixed limit to keep log entries bounded.
   *
   * @param request the current HTTP request
   * @return an immutable map with lowercase header names
   */
  public Map<String, String> capture(HttpServletRequest request) {
    ImmutableMap.Builder<String, String> captured = ImmutableMap.builder();
    for (String name : headerNames) {
      String value = request.getHeader(name);
      if (value != null) {
        int length = Math.min(value.length(), MAX_VALUE_LENGTH);
        StringBuilder safe = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
          char c = value.charAt(i);
          safe.append(Character.isISOControl(c) || c == '\u2028' || c == '\u2029' ? ' ' : c);
        }
        if (value.length() > MAX_VALUE_LENGTH) {
          safe.append(TRUNCATED_SUFFIX);
        }
        captured.put(name, safe.toString());
      }
    }
    return captured.build();
  }

  List<String> headerNames() {
    return headerNames;
  }
}
