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
package org.apache.gravitino.lance.common.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSerializationUtils {

  @Test
  public void testDeserializeProperties() {
    Map<String, String> result =
        SerializationUtils.deserializeProperties("{\"k1\":\"v1\",\"k2\":2,\"k3\":true}");

    Assertions.assertEquals("v1", result.get("k1"));
    Assertions.assertEquals("2", result.get("k2"));
    Assertions.assertEquals("true", result.get("k3"));
    Assertions.assertEquals(3, result.size());
  }

  @Test
  public void testDeserializePropertiesWithBlankInput() {
    Assertions.assertTrue(SerializationUtils.deserializeProperties(" ").isEmpty());
    Assertions.assertTrue(SerializationUtils.deserializeProperties(null).isEmpty());
  }

  @Test
  public void testDeserializePropertiesWithInvalidJson() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SerializationUtils.deserializeProperties("{\"k1\":}"));
    Assertions.assertEquals("Failed to deserialize table properties JSON", exception.getMessage());
    Assertions.assertInstanceOf(JsonProcessingException.class, exception.getCause());
  }
}
