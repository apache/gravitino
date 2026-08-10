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

import static org.apache.gravitino.secret.SecretConstants.ATTR_ENTITY_ID;
import static org.apache.gravitino.secret.SecretConstants.ATTR_ENTITY_TYPE;
import static org.apache.gravitino.secret.SecretConstants.ATTR_PROPERTY_KEY;
import static org.apache.gravitino.secret.SecretConstants.URN_PREFIX;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Value object for a Gravitino secret URN of the form {@code
 * urn:gravitino-secret:<providerName>:<identifier-segments...>}.
 */
@DeveloperApi
public final class SecretUrn {

  // Allow '.' so dotted property keys (e.g. authentication.password) can appear in URNs.
  private static final Pattern SEGMENT_PATTERN = Pattern.compile("[a-zA-Z0-9._-]+");

  private final String providerName;
  private final List<String> identifierSegments;

  private SecretUrn(String providerName, List<String> identifierSegments) {
    this.providerName = providerName;
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
   * Returns the type-specific identifier split into colon-separated segments.
   *
   * @return the identifier segments
   */
  public List<String> identifierSegments() {
    return identifierSegments;
  }

  /**
   * Returns the property key encoded as the last identifier segment.
   *
   * <p>Gravitino secret URNs end with the entity property key that stores the URN (see {@link
   * #buildWriteThrough}). Identifier segments are validated when the URN is parsed or built.
   *
   * @return the property key
   */
  public String propertyKey() {
    return identifierSegments.get(identifierSegments.size() - 1);
  }

  /**
   * Builds a write-through secret URN for an entity property secret.
   *
   * <p>Required attributes: {@link SecretConstants#ATTR_ENTITY_TYPE}, {@link
   * SecretConstants#ATTR_ENTITY_ID}, and {@link SecretConstants#ATTR_PROPERTY_KEY}.
   *
   * @param providerName the configured provider name
   * @param attributes write-through attributes
   * @return the write-through secret URN
   */
  public static SecretUrn buildWriteThrough(String providerName, Map<String, String> attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("attributes must not be null");
    }
    String entityType = requiredAttribute(attributes, ATTR_ENTITY_TYPE);
    String entityId = requiredAttribute(attributes, ATTR_ENTITY_ID);
    String propertyKey = requiredAttribute(attributes, ATTR_PROPERTY_KEY);
    try {
      Long.parseLong(entityId);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "attributes." + ATTR_ENTITY_ID + " must be a numeric entity id: " + entityId, e);
    }
    validateSegment(providerName);
    validateSegment(entityType);
    validateSegment(entityId);
    validateSegment(propertyKey);
    return new SecretUrn(
        providerName,
        Collections.unmodifiableList(Arrays.asList(entityType, entityId, propertyKey)));
  }

  /**
   * Parses a Gravitino secret URN string.
   *
   * @param urn the secret URN string
   * @return the parsed secret URN
   */
  public static SecretUrn parse(String urn) {
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
    return new SecretUrn(segments[0], Collections.unmodifiableList(Arrays.asList(identifierParts)));
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

  private static String requiredAttribute(Map<String, String> attributes, String key) {
    String value = attributes.get(key);
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException("attributes." + key + " must not be blank");
    }
    return value;
  }

  @Override
  public String toString() {
    return URN_PREFIX + providerName + ":" + String.join(":", identifierSegments);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SecretUrn)) {
      return false;
    }
    SecretUrn that = (SecretUrn) other;
    return Objects.equals(providerName, that.providerName)
        && Objects.equals(identifierSegments, that.identifierSegments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(providerName, identifierSegments);
  }
}
