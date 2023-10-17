/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;

public class MapUtils {
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
}
