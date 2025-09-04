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
package org.apache.gravitino;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringIdentifier {

  private static final Logger LOG = LoggerFactory.getLogger(StringIdentifier.class);

  public static final String ID_KEY = "gravitino.identifier";

  /** For test connection only */
  public static final StringIdentifier DUMMY_ID = fromId(-1L);

  @VisibleForTesting static final int CURRENT_FORMAT_VERSION = 1;

  @VisibleForTesting static final String CURRENT_FORMAT = "gravitino.v%d.uid%d";

  private static final String STRING_COMMENT = "From Gravitino, DO NOT EDIT: ";
  private static final String STRING_COMMENT_FORMAT = "%s(%s%s)";

  private static final Pattern CURRENT_FORMAT_REGEX =
      Pattern.compile("gravitino\\.v(\\d+)\\.uid(\\d+)");

  private static final Map<Integer, Pair<String, Pattern>> SUPPORTED_FORMAT =
      ImmutableMap.<Integer, Pair<String, Pattern>>builder()
          .put(CURRENT_FORMAT_VERSION, Pair.of(CURRENT_FORMAT, CURRENT_FORMAT_REGEX))
          .build();

  private final long id;

  private StringIdentifier(long id) {
    this.id = id;
  }

  public static StringIdentifier fromId(long id) {
    return new StringIdentifier(id);
  }

  public static StringIdentifier fromString(String idString) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(idString), "Input id string cannot be null or empty");

    for (Map.Entry<Integer, Pair<String, Pattern>> entry : SUPPORTED_FORMAT.entrySet()) {
      int supportedVersion = entry.getKey();
      Pair<String, Pattern> format = entry.getValue();
      Pattern pattern = format.getRight();

      Matcher m = pattern.matcher(idString);
      if (m.matches()) {
        int version = Integer.parseInt(m.group(1));
        long id = Long.parseLong(m.group(2));

        if (version != supportedVersion) {
          continue;
        }

        return new StringIdentifier(id);
      }
    }

    throw new IllegalArgumentException("Invalid string identifier format: " + idString);
  }

  public long id() {
    return id;
  }

  @Override
  public String toString() {
    return String.format(CURRENT_FORMAT, CURRENT_FORMAT_VERSION, id);
  }

  /**
   * Helper methods to set/get StringIdentifier to/from different places
   *
   * @param stringId the string identifier to add to the properties
   * @param properties the properties to add the string identifier to
   * @return the properties with the string identifier added
   */
  public static Map<String, String> newPropertiesWithId(
      StringIdentifier stringId, Map<String, String> properties) {
    if (properties == null) {
      return ImmutableMap.of(ID_KEY, stringId.toString());
    }

    if (properties.containsKey(ID_KEY)) {
      if (LOG.isWarnEnabled()) {
        LOG.warn(
            "Property {}:{} already existed in the properties, this is unexpected, we will "
                + "ignore adding the identifier to the properties",
            ID_KEY,
            properties.get(ID_KEY));
      }
      return Collections.unmodifiableMap(properties);
    }

    return ImmutableMap.<String, String>builder()
        .putAll(properties)
        .put(ID_KEY, stringId.toString())
        .build();
  }

  /**
   * Remove StringIdentifier from properties.
   *
   * @param properties the properties to remove the string identifier from.
   * @return the properties with the string identifier removed.
   */
  public static Map<String, String> newPropertiesWithoutId(Map<String, String> properties) {
    if (properties == null) {
      return null;
    }

    if (!properties.containsKey(ID_KEY)) {
      return Collections.unmodifiableMap(properties);
    }

    Map<String, String> copy = Maps.newHashMap(properties);
    copy.remove(ID_KEY);

    return ImmutableMap.<String, String>builder().putAll(copy).build();
  }

  public static StringIdentifier fromProperties(Map<String, String> properties) {
    if (properties == null) {
      return null;
    }

    String idString = properties.get(ID_KEY);
    if (StringUtils.isBlank(idString)) {
      return null;
    }

    return fromString(idString);
  }

  public static String addToComment(StringIdentifier stringId, String comment) {
    if (StringUtils.isBlank(comment)) {
      return String.format(STRING_COMMENT_FORMAT, "", STRING_COMMENT, stringId.toString());
    }

    return String.format(
        STRING_COMMENT_FORMAT, comment.trim() + " ", STRING_COMMENT, stringId.toString());
  }

  public static StringIdentifier fromComment(String comment) {
    if (StringUtils.isBlank(comment)) {
      return null;
    }

    String pattern = "(" + STRING_COMMENT;
    int startIndex = comment.indexOf(pattern);
    if (startIndex == -1) {
      return null;
    }

    String commentSubstring = comment.substring(startIndex);

    int left = 0;
    int right = commentSubstring.indexOf(')', pattern.length());
    if (right == -1) {
      return null;
    }

    String innerComment = commentSubstring.substring(left + 1, right);
    if (!innerComment.startsWith(STRING_COMMENT)) {
      return null;
    }

    String idString = innerComment.substring(STRING_COMMENT.length());
    if (idString.isEmpty()) {
      return null;
    }

    return fromString(idString);
  }

  public static String removeIdFromComment(String comment) {
    if (StringUtils.isBlank(comment)) {
      return comment;
    }

    StringIdentifier identifier = fromComment(comment);
    if (identifier != null) {
      String format = String.format(STRING_COMMENT_FORMAT, " ", STRING_COMMENT, identifier);
      int indexOf = comment.indexOf(format);

      if (indexOf == -1) {
        format = String.format(STRING_COMMENT_FORMAT, "", STRING_COMMENT, identifier);
        indexOf = comment.indexOf(format);
      }

      if (indexOf != -1) {
        return comment.substring(0, indexOf).trim();
      }
    }
    return comment;
  }
}
