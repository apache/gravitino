/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastrato.graviton.rest.RESTUtils;
import com.google.common.collect.ImmutableMap;
import java.io.UncheckedIOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestRESTUtils {

  @Test
  void testStripTrailingSlash() {
    assertEquals("/path/to/resource", RESTUtils.stripTrailingSlash("/path/to/resource/"));
    assertEquals("/path/to/resource", RESTUtils.stripTrailingSlash("/path/to/resource////"));
    assertEquals("", RESTUtils.stripTrailingSlash(""));
    assertEquals(null, RESTUtils.stripTrailingSlash(null));
  }

  @Test
  void testEncodeFormData() {
    Map<Object, Object> formData =
        ImmutableMap.builder().put("key1", "value1").put("key2", "value2").build();

    String expected = "key1=value1&key2=value2";
    assertEquals(expected, RESTUtils.encodeFormData(formData));

    Map<Object, Object> emptyFormData = ImmutableMap.of();
    assertEquals("", RESTUtils.encodeFormData(emptyFormData));
  }

  @Test
  void testDecodeFormData() {
    String formString = "key1=value1&key2=value2";
    Map<Object, Object> expectedFormData =
        ImmutableMap.builder().put("key1", "value1").put("key2", "value2").build();

    assertEquals(expectedFormData, RESTUtils.decodeFormData(formString));

    String emptyFormString = "";
    Map<String, String> emptyFormData = ImmutableMap.of();
    assertEquals(emptyFormData, RESTUtils.decodeFormData(emptyFormString));
  }

  @Test
  void testEncodeString() {
    assertEquals("hello%20world", RESTUtils.encodeString("hello world"));
    assertEquals("test", RESTUtils.encodeString("test"));
    assertEquals("", RESTUtils.encodeString(""));
    assertThrows(UncheckedIOException.class, () -> RESTUtils.encodeString(null));
  }

  @Test
  void testDecodeString() {
    assertEquals("hello world", RESTUtils.decodeString("hello%20world"));
    assertEquals("test", RESTUtils.decodeString("test"));
    assertEquals("", RESTUtils.decodeString(""));
    assertThrows(UncheckedIOException.class, () -> RESTUtils.decodeString(null));
  }
}
