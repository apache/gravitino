/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.trino.connector.util.json;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;

/**
 * Placeholder shape for Trino 482+, where {@code io.trino.spi.type.TypeSignature} was removed.
 *
 * <p>{@link JsonCodec} only registers a TypeSignature deserializer after detecting that {@code
 * TypeSignature} exists on the runtime SPI, so this class is never registered or instantiated on
 * these versions; it exists solely so the shared {@link JsonCodec} source keeps compiling. Trino
 * 440-481 ship the functional compile-time shape instead.
 */
public final class TypeSignatureDeserializer extends FromStringDeserializer<Object> {

  /**
   * Constructs a new TypeSignatureDeserializer.
   *
   * @param classLoader the class loader that would be used to load the type signature translator
   *     class
   * @throws UnsupportedOperationException always, on Trino 482+
   */
  public TypeSignatureDeserializer(ClassLoader classLoader) {
    super(Object.class);
    throw new UnsupportedOperationException(
        "TypeSignature was removed from the Trino SPI in 482; its deserializer is never used.");
  }

  @Override
  protected Object _deserialize(String value, DeserializationContext context) {
    throw new UnsupportedOperationException(
        "TypeSignature was removed from the Trino SPI in 482; its deserializer is never used.");
  }
}
