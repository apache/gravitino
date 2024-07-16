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
package org.apache.gravitino.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
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

  @Test
  void testFindAvailablePort() throws IOException {
    assertTrue(RESTUtils.findAvailablePort(0, 0) > 0);
    assertThrows(IOException.class, () -> RESTUtils.findAvailablePort(3000, 2000));

    // valid user registered port https://en.wikipedia.org/wiki/Registered_port
    // port number must in [1024, 65535]
    assertThrows(IOException.class, () -> RESTUtils.findAvailablePort(1024 - 1, 65535));
    assertThrows(IOException.class, () -> RESTUtils.findAvailablePort(1024, 65535 + 1));
    assertThrows(IOException.class, () -> RESTUtils.findAvailablePort(1024 - 1, 65535 + 1));
    assertThrows(
        IOException.class, () -> RESTUtils.findAvailablePort(Integer.MIN_VALUE, Integer.MAX_VALUE));
    assertThrows(
        IOException.class, () -> RESTUtils.findAvailablePort(Integer.MAX_VALUE, Integer.MIN_VALUE));

    int port1 = RESTUtils.findAvailablePort(12345, 12345);
    assertEquals(12345, port1);

    int startPort = 20000;
    int loop = 10;
    for (int i = 0; i < loop; i++) {
      int port = RESTUtils.findAvailablePort(startPort, startPort + i);
      assertTrue(port >= startPort && port <= startPort + i);
    }
  }
}
