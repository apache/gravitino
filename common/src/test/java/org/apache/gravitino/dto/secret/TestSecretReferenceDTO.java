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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretReferenceDTO {

  @Test
  void testAttributesRequired() {
    SecretReferenceDTO dto = SecretReferenceDTO.builder().withProvider("vault").build();
    Assertions.assertNotNull(dto.getAttributes());
    Assertions.assertTrue(dto.getAttributes().isEmpty());
    Assertions.assertThrows(IllegalArgumentException.class, dto::toSecretReference);

    SecretReference reference =
        SecretReferenceDTO.builder()
            .withProvider("vault")
            .withAttributes(Map.of("path", "secret/data/x"))
            .build()
            .toSecretReference();
    Assertions.assertEquals("vault", reference.provider());
    Assertions.assertEquals("secret/data/x", reference.attributes().get("path"));
  }

  @Test
  void testMapConverters() {
    Assertions.assertEquals(ImmutableMap.of(), SecretReferenceDTO.toSecretReferences(null));
    Assertions.assertEquals(ImmutableMap.of(), SecretReferenceDTO.fromSecretReferences(null));
    Assertions.assertEquals(ImmutableMap.of(), SecretBindingDTO.toSecretBindings(null));
    Assertions.assertEquals(ImmutableMap.of(), SecretBindingDTO.fromSecretBindings(null));

    Map<String, SecretReference> references =
        Map.of("jdbc-password", new SecretReference("vault", Map.of("path", "secret/data/x")));
    Map<String, SecretReferenceDTO> dtos = SecretReferenceDTO.fromSecretReferences(references);
    Assertions.assertEquals(1, dtos.size());
    Assertions.assertEquals(references, SecretReferenceDTO.toSecretReferences(dtos));

    Map<String, SecretBinding> bindings =
        Map.of("jdbc-password", new SecretBinding("memory", "s3cr3t"));
    Map<String, SecretBindingDTO> bindingDtos = SecretBindingDTO.fromSecretBindings(bindings);
    Assertions.assertEquals(1, bindingDtos.size());
    Assertions.assertEquals(bindings, SecretBindingDTO.toSecretBindings(bindingDtos));
  }

  @Test
  void testNullMapValues() {
    Map<String, SecretReferenceDTO> nullRef = new HashMap<>();
    nullRef.put("jdbc-password", null);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SecretReferenceDTO.toSecretReferences(nullRef));

    Map<String, SecretBindingDTO> nullBinding = new HashMap<>();
    nullBinding.put("jdbc-password", null);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SecretBindingDTO.toSecretBindings(nullBinding));
  }
}
