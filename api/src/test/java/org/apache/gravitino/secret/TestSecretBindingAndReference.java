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

package org.apache.gravitino.secret;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretBindingAndReference {

  @Test
  void testSecretBinding() {
    SecretBinding binding = new SecretBinding("memory", "s3cr3t");
    Assertions.assertEquals("memory", binding.provider());
    Assertions.assertEquals("s3cr3t", binding.value());
    Assertions.assertFalse(binding.toString().contains("s3cr3t"));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new SecretBinding("memory", "******"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new SecretBinding("", "x"));
  }

  @Test
  void testSecretReference() {
    SecretReference reference =
        new SecretReference("vault", Map.of("path", "secret/data/x", "key", "password"));
    Assertions.assertEquals("vault", reference.provider());
    Assertions.assertEquals("secret/data/x", reference.attributes().get("path"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new SecretReference(
                "vault", Map.of("path", "urn:gravitino-secret:memory:catalog:1:jdbc-password")));

    SecretReference emptyAttrs = new SecretReference("vault", null);
    Assertions.assertTrue(emptyAttrs.attributes().isEmpty());
  }
}
