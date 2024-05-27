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
package com.datastrato.gravitino.trino.connector.util.json;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import java.util.function.Function;

/**
 * This class is reference to Trino source code io.trino.plugin.base.TypeDeserializer, It use to
 * handle Type serialization
 */
public final class TypeDeserializer extends FromStringDeserializer<Type> {
  private final Function<TypeId, Type> typeLoader;

  public TypeDeserializer(TypeManager typeManager) {
    this(typeManager::getType);
  }

  public TypeDeserializer(Function<TypeId, Type> typeLoader) {
    super(Type.class);
    this.typeLoader = requireNonNull(typeLoader, "typeLoader is null");
  }

  @Override
  protected Type _deserialize(String value, DeserializationContext context) {
    return typeLoader.apply(TypeId.of(value));
  }
}
