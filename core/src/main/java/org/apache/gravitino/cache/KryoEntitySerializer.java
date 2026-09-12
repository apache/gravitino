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
package org.apache.gravitino.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Serializes entities for {@link RedisEntityCache} with Kryo.
 *
 * <p>Entities are plain objects with private fields and no public constructors, so this uses
 * Objenesis to instantiate them and {@link CompatibleFieldSerializer} to write field names, which
 * keeps a value written by one server version readable by another during a rolling upgrade: an
 * unknown field is skipped and a missing one stays at its default. Classes are written by name, so
 * no registration is needed. Two families of objects need custom handling:
 *
 * <ul>
 *   <li>{@link Type} values are written in the JSON form used by the REST API, because the
 *       primitive types are singletons compared by identity and reading them back through {@link
 *       JsonUtils} returns those singletons.
 *   <li>Guava immutable collections keep their contents in transient fields, so they are written as
 *       their elements and rebuilt on read.
 *   <li>{@link Column#DEFAULT_VALUE_NOT_SET} is a lambda, whose class has a JVM-specific hidden
 *       name, so it is registered under a fixed id and written as a marker that reads back as the
 *       same instance.
 * </ul>
 *
 * <p>A value that cannot be read back, for example after an incompatible class change, surfaces as
 * a {@link KryoException} or {@link ClassCastException}; the cache treats that as a miss and
 * discards the entry.
 */
class KryoEntitySerializer {

  private static final int INITIAL_BUFFER_SIZE = 1024;
  private static final int POOL_SIZE = 32;

  /** Registration id of the {@link Column#DEFAULT_VALUE_NOT_SET} marker; above Kryo's own ids. */
  private static final int DEFAULT_VALUE_NOT_SET_ID = 100;

  private static final ObjectMapper TYPE_MAPPER =
      JsonUtils.objectMapper()
          .copy()
          .registerModule(
              new SimpleModule("gravitino-cache-types")
                  .addSerializer(Type.class, new JsonUtils.TypeSerializer())
                  .addDeserializer(Type.class, new JsonUtils.TypeDeserializer()));

  private final Pool<Kryo> pool =
      new Pool<Kryo>(true, false, POOL_SIZE) {
        @Override
        protected Kryo create() {
          return newKryo();
        }
      };

  /**
   * Serializes an entity.
   *
   * @param entity The entity to serialize
   * @return The serialized bytes
   */
  byte[] serialize(Entity entity) {
    Kryo kryo = pool.obtain();
    try (Output output = new Output(INITIAL_BUFFER_SIZE, -1)) {
      kryo.writeClassAndObject(output, entity);
      return output.toBytes();
    } finally {
      pool.free(kryo);
    }
  }

  /**
   * Deserializes an entity written by {@link #serialize(Entity)}.
   *
   * @param bytes The serialized bytes
   * @return The entity
   * @throws KryoException if the bytes cannot be read back as an entity
   */
  Entity deserialize(byte[] bytes) {
    Kryo kryo = pool.obtain();
    try (Input input = new Input(bytes)) {
      Object value = kryo.readClassAndObject(input);
      if (!(value instanceof Entity)) {
        throw new KryoException(
            "Expected an Entity but read " + (value == null ? "null" : value.getClass().getName()));
      }
      return (Entity) value;
    } finally {
      pool.free(kryo);
    }
  }

  private static Kryo newKryo() {
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(false);
    kryo.setReferences(true);
    kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    kryo.addDefaultSerializer(Type.class, new TypeSerializer());
    kryo.addDefaultSerializer(ImmutableList.class, new ImmutableListSerializer());
    kryo.addDefaultSerializer(ImmutableSet.class, new ImmutableSetSerializer());
    kryo.addDefaultSerializer(ImmutableMap.class, new ImmutableMapSerializer());
    kryo.register(
        Column.DEFAULT_VALUE_NOT_SET.getClass(),
        new SingletonSerializer<>(Column.DEFAULT_VALUE_NOT_SET),
        DEFAULT_VALUE_NOT_SET_ID);
    return kryo;
  }

  /** Writes nothing and reads back a fixed instance. */
  private static final class SingletonSerializer<T> extends Serializer<T> {
    private final T instance;

    SingletonSerializer(T instance) {
      super(false, true);
      this.instance = instance;
    }

    @Override
    public void write(Kryo kryo, Output output, T value) {}

    @Override
    public T read(Kryo kryo, Input input, Class<? extends T> clazz) {
      return instance;
    }
  }

  /** Writes a {@link Type} as its REST JSON form so that reading it back yields the singletons. */
  private static final class TypeSerializer extends Serializer<Type> {
    TypeSerializer() {
      super(false, true);
    }

    @Override
    public void write(Kryo kryo, Output output, Type type) {
      try {
        output.writeString(TYPE_MAPPER.writeValueAsString(type));
      } catch (IOException e) {
        throw new KryoException("Failed to serialize type " + type, e);
      }
    }

    @Override
    public Type read(Kryo kryo, Input input, Class<? extends Type> clazz) {
      String json = input.readString();
      try {
        return TYPE_MAPPER.readValue(json, Type.class);
      } catch (IOException e) {
        throw new KryoException("Failed to deserialize type " + json, e);
      }
    }
  }

  private static final class ImmutableListSerializer extends Serializer<ImmutableList<?>> {
    ImmutableListSerializer() {
      super(false, true);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableList<?> list) {
      output.writeVarInt(list.size(), true);
      for (Object element : list) {
        kryo.writeClassAndObject(output, element);
      }
    }

    @Override
    public ImmutableList<?> read(Kryo kryo, Input input, Class<? extends ImmutableList<?>> clazz) {
      int size = input.readVarInt(true);
      ImmutableList.Builder<Object> builder = ImmutableList.builderWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        builder.add(kryo.readClassAndObject(input));
      }
      return builder.build();
    }
  }

  private static final class ImmutableSetSerializer extends Serializer<ImmutableSet<?>> {
    ImmutableSetSerializer() {
      super(false, true);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableSet<?> set) {
      output.writeVarInt(set.size(), true);
      for (Object element : set) {
        kryo.writeClassAndObject(output, element);
      }
    }

    @Override
    public ImmutableSet<?> read(Kryo kryo, Input input, Class<? extends ImmutableSet<?>> clazz) {
      int size = input.readVarInt(true);
      ImmutableSet.Builder<Object> builder = ImmutableSet.builderWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        builder.add(kryo.readClassAndObject(input));
      }
      return builder.build();
    }
  }

  private static final class ImmutableMapSerializer extends Serializer<ImmutableMap<?, ?>> {
    ImmutableMapSerializer() {
      super(false, true);
    }

    @Override
    public void write(Kryo kryo, Output output, ImmutableMap<?, ?> map) {
      output.writeVarInt(map.size(), true);
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        kryo.writeClassAndObject(output, entry.getKey());
        kryo.writeClassAndObject(output, entry.getValue());
      }
    }

    @Override
    public ImmutableMap<?, ?> read(
        Kryo kryo, Input input, Class<? extends ImmutableMap<?, ?>> clazz) {
      int size = input.readVarInt(true);
      ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builderWithExpectedSize(size);
      for (int i = 0; i < size; i++) {
        Object key = kryo.readClassAndObject(input);
        Object value = kryo.readClassAndObject(input);
        builder.put(key, value);
      }
      return builder.build();
    }
  }
}
