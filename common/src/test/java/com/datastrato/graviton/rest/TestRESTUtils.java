/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
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

    /* This may not be behaviour we want? */
    assertThrows(IllegalArgumentException.class, () -> RESTUtils.decodeFormData(emptyFormString));
  }

  @Test
  void testEncodeString() {
    assertEquals("test", RESTUtils.encodeString("test"));
    assertEquals("", RESTUtils.encodeString(""));
    /* not %20 as you might expect */
    assertEquals("hello+world", RESTUtils.encodeString("hello world"));
    assertThrows(IllegalArgumentException.class, () -> RESTUtils.encodeString(null));
  }

  @Test
  void testDecodeString() {
    assertEquals("test", RESTUtils.decodeString("test"));
    assertEquals("", RESTUtils.decodeString(""));
    assertEquals("hello world", RESTUtils.decodeString("hello%20world"));
    assertThrows(IllegalArgumentException.class, () -> RESTUtils.decodeString(null));
  }
}
