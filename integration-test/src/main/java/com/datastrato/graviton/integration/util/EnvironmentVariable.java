/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.integration.util;

import java.lang.reflect.Field;
import java.util.Map;

public class EnvironmentVariable {
  public static void injectEnv(String key, String value) throws Exception {
    Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

    Field unmodifiableMapField =
        getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
    Object unmodifiableMap = unmodifiableMapField.get(null);
    injectIntoUnmodifiableMap(key, value, unmodifiableMap);

    Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
    Map<String, String> map = (Map<String, String>) mapField.get(null);
    map.put(key, value);
  }

  private static Field getAccessibleField(Class<?> clazz, String fieldName)
      throws NoSuchFieldException {
    Field field = clazz.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  private static void injectIntoUnmodifiableMap(String key, String value, Object map)
      throws ReflectiveOperationException {
    Class<?> unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
    Field field = getAccessibleField(unmodifiableMap, "m");
    Object obj = field.get(map);

    ((Map<String, String>) obj).put(key, value);
  }
}
