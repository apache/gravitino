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
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.Set;

/**
 * This class is reference to Trino source code io.trino.type.TypeSignatureDeserializer, use
 * refactoring to call the key method to handle Type serialization.
 *
 * <p>Trino 482 removed {@code io.trino.spi.type.TypeSignature}, so this deserializer avoids
 * referencing the class at compile time and resolves everything reflectively. It is only registered
 * on Trino versions that still expose {@code TypeSignature} (see {@code
 * JsonCodec#registerTypeSignatureDeserializer}); on newer versions it is never instantiated.
 */
public final class TypeSignatureDeserializer extends FromStringDeserializer<Object> {
  /** Method to parse type signatures using reflection. */
  private final Method parseTypeSignatureMethod;

  /**
   * Constructs a new TypeSignatureDeserializer.
   *
   * @param classLoader the class loader to use for loading the type signature translator class
   * @throws RuntimeException if the type signature translator class cannot be loaded or the parse
   *     method cannot be found
   */
  public TypeSignatureDeserializer(ClassLoader classLoader) {
    super(Object.class);
    try {
      Class<?> clazz = classLoader.loadClass("io.trino.sql.analyzer.TypeSignatureTranslator");
      parseTypeSignatureMethod =
          clazz.getDeclaredMethod("parseTypeSignature", String.class, Set.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Object _deserialize(String value, DeserializationContext context) {
    try {
      return parseTypeSignatureMethod.invoke(null, value, ImmutableSet.of());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
