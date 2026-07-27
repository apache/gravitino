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

import static org.apache.gravitino.secret.SecretConstants.URN_PREFIX;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;

/** Helpers for building and parsing Gravitino secret URNs. */
@DeveloperApi
public final class SecretUrn {

  // Allow '.' so dotted property keys (e.g. authentication.password) can appear in URNs.
  private static final Pattern SEGMENT_PATTERN = Pattern.compile("[a-zA-Z0-9._-]+");

  /** Parsed representation of a Gravitino secret URN. */
  public static final class ParsedUrn {
    private final String providerName;
    private final String identifier;
    private final List<String> identifierSegments;

    private ParsedUrn(String providerName, String identifier, List<String> identifierSegments) {
      this.providerName = providerName;
      this.identifier = identifier;
      this.identifierSegments = identifierSegments;
    }

    /**
     * Returns the provider name from the URN.
     *
     * @return the provider name
     */
    public String providerName() {
      return providerName;
    }

    /**
     * Returns the type-specific identifier portion of the URN.
     *
     * @return the identifier
     */
    public String identifier() {
      return identifier;
    }

    /**
     * Returns the identifier split into colon-separated segments.
     *
     * @return the identifier segments
     */
    public List<String> identifierSegments() {
      return identifierSegments;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ParsedUrn)) {
        return false;
      }
      ParsedUrn that = (ParsedUrn) other;
      return Objects.equals(providerName, that.providerName)
          && Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
      return Objects.hash(providerName, identifier);
    }
  }

  private SecretUrn() {}

  /**
   * Builds a write-through URN for an entity property secret.
   *
   * @param providerName the configured provider name
   * @param entityType the entity type
   * @param entityId the entity identifier
   * @param propertyKey the property key
   * @return the write-through URN
   */
  public static String buildWriteThrough(
      String providerName, String entityType, long entityId, String propertyKey) {
    validateSegment(providerName);
    validateSegment(entityType);
    validateSegment(String.valueOf(entityId));
    validateSegment(propertyKey);
    return URN_PREFIX + providerName + ":" + entityType + ":" + entityId + ":" + propertyKey;
  }

  /**
   * Parses a Gravitino secret URN.
   *
   * @param urn the secret URN
   * @return the parsed URN
   */
  public static ParsedUrn parse(String urn) {
    if (!StringUtils.startsWith(urn, URN_PREFIX)) {
      throw new IllegalArgumentException("Invalid Gravitino secret URN: " + urn);
    }

    String remainder = urn.substring(URN_PREFIX.length());
    if (StringUtils.isEmpty(remainder)) {
      throw new IllegalArgumentException("Invalid Gravitino secret URN: " + urn);
    }

    String[] segments = remainder.split(":", -1);
    if (segments.length < 2) {
      throw new IllegalArgumentException("Invalid Gravitino secret URN: " + urn);
    }

    for (String segment : segments) {
      validateSegment(segment);
    }

    String[] identifierParts = Arrays.copyOfRange(segments, 1, segments.length);
    String providerName = segments[0];
    String identifier = StringUtils.join(identifierParts, ':');
    List<String> identifierSegments = Collections.unmodifiableList(Arrays.asList(identifierParts));
    return new ParsedUrn(providerName, identifier, identifierSegments);
  }

  /**
   * Validates that a URN segment contains only allowed characters.
   *
   * @param segment the segment to validate
   */
  public static void validateSegment(String segment) {
    if (StringUtils.isEmpty(segment) || !SEGMENT_PATTERN.matcher(segment).matches()) {
      throw new IllegalArgumentException("Invalid URN segment: " + segment);
    }
  }
}
