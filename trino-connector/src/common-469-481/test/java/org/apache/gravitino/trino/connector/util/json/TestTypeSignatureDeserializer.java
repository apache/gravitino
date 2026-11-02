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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.trino.spi.type.TypeSignature;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TypeSignatureDeserializer} on Trino versions that expose TypeSignature. */
class TestTypeSignatureDeserializer {

  private static final ClassLoader CLASS_LOADER =
      TestTypeSignatureDeserializer.class.getClassLoader();

  @Test
  void testRegisteredDeserializerParsesTheInputValue() throws Exception {
    SimpleModule module = new SimpleModule();
    TypeSignatureDeserializer.register(module, CLASS_LOADER);
    ObjectMapper mapper = new ObjectMapper().registerModule(module);

    // The deserializer must parse its input, not a hardcoded signature.
    assertThat(mapper.readValue("\"varchar(10)\"", TypeSignature.class)).hasToString("varchar(10)");
    assertThat(mapper.readValue("\"integer\"", TypeSignature.class)).hasToString("integer");
  }

  @Test
  void testJsonCodecMapperRegistersTheDeserializer() throws Exception {
    ObjectMapper mapper = JsonCodec.createMapper(JsonCodec.createTypeManager(CLASS_LOADER));

    assertThat(mapper.readValue("\"varchar(10)\"", TypeSignature.class)).hasToString("varchar(10)");
  }
}
