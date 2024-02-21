/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

/** Utility class for working with maps. */
public class MapUtils {
  private static final String PROPERTIES_REDACTION_PATTERN = "(?i)secret|password|token";
  private static final Pattern PROPERTIES_REDACTION = Pattern.compile(PROPERTIES_REDACTION_PATTERN);

  @VisibleForTesting static final String REDACTION_REPLACEMENT_TEXT = "*********(redacted)";

  /**
   * Returns a map with all keys that start with the given prefix.
   *
   * @param m The map to filter.
   * @param prefix The prefix to filter by.
   * @return A map with all keys that start with the given prefix.
   */
  public static Map<String, String> getPrefixMap(Map<String, String> m, String prefix) {
    Map<String, String> configs = Maps.newHashMap();
    m.forEach(
        (k, v) -> {
          if (k.startsWith(prefix)) {
            String newKey = k.substring(prefix.length());
            configs.put(newKey, v);
          }
        });

    return Collections.unmodifiableMap(configs);
  }

  /**
   * Returns an unmodifiable map.
   *
   * @param m The map to make unmodifiable.
   * @return An unmodifiable map.
   */
  public static Map<String, String> unmodifiableMap(Map<String, String> m) {
    return Collections.unmodifiableMap(m);
  }

  /**
   * Returns a redacted map.
   *
   * @param source The map to redact.
   * @return Map.
   */
  public static Map<String, String> redactSensitiveValueByKey(Map<String, String> source) {
    Map<String, String> redactedMap = Maps.newHashMap(source);
    redactedMap.replaceAll(
        (k, v) -> {
          if (PROPERTIES_REDACTION.matcher(k).find()) {
            return REDACTION_REPLACEMENT_TEXT;
          }
          return v;
        });
    return redactedMap;
  }
}
