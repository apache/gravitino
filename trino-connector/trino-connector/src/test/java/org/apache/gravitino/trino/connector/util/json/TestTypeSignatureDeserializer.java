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
package org.apache.gravitino.trino.connector.util.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TypeSignatureDeserializer}. Trino 482 removed {@code
 * io.trino.spi.type.TypeSignature} (and {@code TypeSignatureTranslator}); on those versions the
 * deserializer is never registered, so these tests skip when the translator class is absent.
 */
class TestTypeSignatureDeserializer {

  private static final String TRANSLATOR_CLASS = "io.trino.sql.analyzer.TypeSignatureTranslator";

  @Test
  void testDeserializeParsesTheInputValue() {
    ClassLoader classLoader = getClass().getClassLoader();
    assumeTrue(isPresent(classLoader, TRANSLATOR_CLASS), "TypeSignature not present on this Trino");

    TypeSignatureDeserializer deserializer = new TypeSignatureDeserializer(classLoader);

    // Regression guard: the deserializer must parse its input, not a hardcoded signature.
    assertThat(deserializer._deserialize("varchar(10)", null)).hasToString("varchar(10)");
    assertThat(deserializer._deserialize("integer", null)).hasToString("integer");
  }

  private static boolean isPresent(ClassLoader classLoader, String className) {
    try {
      classLoader.loadClass(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
