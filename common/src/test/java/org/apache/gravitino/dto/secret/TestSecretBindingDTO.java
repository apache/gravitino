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
package org.apache.gravitino.dto.secret;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** SerDe tests for {@link SecretBindingDTO}, including scalar-rejection without plaintext echo. */
public class TestSecretBindingDTO {

  private static final ObjectMapper MAPPER = JsonUtils.objectMapper();

  @Test
  void testObjectRoundTrip() throws Exception {
    SecretBindingDTO dto =
        SecretBindingDTO.builder().withProvider("memory").withPlaintext("s3cr3t").build();
    String json = MAPPER.writeValueAsString(dto);
    SecretBindingDTO parsed = MAPPER.readValue(json, SecretBindingDTO.class);
    Assertions.assertEquals("memory", parsed.getProvider());
    Assertions.assertEquals("s3cr3t", parsed.getPlaintext());
  }

  @Test
  void testScalarBindingDoesNotEchoPlaintext() {
    String secret = "MY-SECRET-VALUE-123";
    JsonMappingException ex =
        Assertions.assertThrows(
            JsonMappingException.class,
            () -> MAPPER.readValue("\"" + secret + "\"", SecretBindingDTO.class));
    Assertions.assertFalse(ex.getMessage().contains(secret));
    Assertions.assertTrue(ex.getMessage().contains("JSON objects"));
  }

  @Test
  void testArrayPlaintextRejectedWithoutEcho() {
    String secret = "LIST-SECRET-999";
    JsonProcessingException ex =
        Assertions.assertThrows(
            JsonProcessingException.class,
            () ->
                MAPPER.readValue(
                    "{\"provider\":\"memory\",\"plaintext\":[\"" + secret + "\"]}",
                    SecretBindingDTO.class));
    Assertions.assertFalse(ex.getMessage().contains(secret));
  }
}
