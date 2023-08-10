/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.proto;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.EntitySerDe;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/** The ProtoEntitySerDe class provides serialization and deserialization of entities. */
public class ProtoEntitySerDe implements EntitySerDe {

  // Entities classes must register in this map to ensure deserialization by ProtoEntitySerDe.
  private static final Map<String, String> ENTITY_TO_SERDE =
      ImmutableMap.of(
          "com.datastrato.graviton.meta.AuditInfo",
          "com.datastrato.graviton.proto.AuditInfoSerDe",
          "com.datastrato.graviton.meta.BaseMetalake",
          "com.datastrato.graviton.proto.BaseMetalakeSerDe",
          "com.datastrato.graviton.meta.CatalogEntity",
          "com.datastrato.graviton.proto.CatalogEntitySerDe");

  private static final Map<String, String> ENTITY_TO_PROTO =
      ImmutableMap.of(
          "com.datastrato.graviton.meta.AuditInfo", "com.datastrato.graviton.proto.AuditInfo",
          "com.datastrato.graviton.meta.BaseMetalake", "com.datastrato.graviton.proto.Metalake",
          "com.datastrato.graviton.meta.CatalogEntity", "com.datastrato.graviton.proto.Catalog");

  private final Map<Class<? extends Entity>, ProtoSerDe<? extends Entity, ? extends Message>>
      entityToSerDe;

  private final Map<Class<? extends Entity>, Class<? extends Message>> entityToProto;

  private final Map<Class<? extends Message>, Class<? extends Entity>> protoToEntity;

  /**
   * Constructor for ProtoEntitySerDe. Initializes mapping structures and loads classes.
   *
   * @throws IOException if class loading or instantiation fails.
   */
  public ProtoEntitySerDe() throws IOException {
    ClassLoader loader =
        Optional.ofNullable(Thread.currentThread().getContextClassLoader())
            .orElse(getClass().getClassLoader());

    // TODO. This potentially has issues in creating serde objects, because the class load here
    //  may have no context for entities which are implemented in the specific catalog module. We
    //  should lazily create the serde class in the classloader when serializing and deserializing.
    this.entityToSerDe = Maps.newHashMap();
    for (Map.Entry<String, String> entry : ENTITY_TO_SERDE.entrySet()) {
      String key = entry.getKey();
      String s = entry.getValue();
      Class<? extends Entity> entityClass = (Class<? extends Entity>) loadClass(key, loader);
      Class<? extends ProtoSerDe<? extends Entity, ? extends Message>> serdeClass =
          (Class<? extends ProtoSerDe<? extends Entity, ? extends Message>>) loadClass(s, loader);

      try {
        ProtoSerDe<? extends Entity, ? extends Message> serde = serdeClass.newInstance();
        entityToSerDe.put(entityClass, serde);
      } catch (Exception exception) {
        throw new IOException("Failed to instantiate serde class " + s, exception);
      }
    }

    this.entityToProto = Maps.newHashMap();
    this.protoToEntity = Maps.newHashMap();
    for (Map.Entry<String, String> entry : ENTITY_TO_PROTO.entrySet()) {
      String e = entry.getKey();
      String p = entry.getValue();
      Class<? extends Entity> entityClass = (Class<? extends Entity>) loadClass(e, loader);
      Class<? extends Message> protoClass = (Class<? extends Message>) loadClass(p, loader);
      entityToProto.put(entityClass, protoClass);
      protoToEntity.put(protoClass, entityClass);
    }
  }

  /**
   * Serializes an entity into a byte array.
   *
   * @param t The entity to be serialized.
   * @param <T> The type of the entity.
   * @return The serialized byte array.
   * @throws IOException If there's an error during serialization.
   */
  @Override
  public <T extends Entity> byte[] serialize(T t) throws IOException {
    Any any = Any.pack(toProto(t));
    return any.toByteArray();
  }

  /**
   * Deserializes a byte array into an entity of the specified class.
   *
   * @param bytes The serialized byte array.
   * @param clazz The class of the entity to be deserialized.
   * @param classLoader The class loader to use for deserialization.
   * @param <T> The type of the entity.
   * @return The deserialized entity.
   * @throws IOException If there's an error during deserialization or if the entity class or proto
   *     class is not registered.
   */
  @Override
  public <T extends Entity> T deserialize(byte[] bytes, Class<T> clazz, ClassLoader classLoader)
      throws IOException {
    Any any = Any.parseFrom(bytes);

    if (!entityToSerDe.containsKey(clazz) || !entityToProto.containsKey(clazz)) {
      throw new IOException("No proto and SerDe class found for entity " + clazz.getName());
    }

    if (!any.is(entityToProto.get(clazz))) {
      throw new IOException("Invalid proto for entity " + clazz.getName());
    }

    try {
      Class<? extends Message> protoClazz = entityToProto.get(clazz);
      Message anyMessage = any.unpack(protoClazz);
      return fromProto(anyMessage);
    } catch (Exception e) {
      throw new IOException("Failed to deserialize entity " + clazz.getName(), e);
    }
  }

  /**
   * Converts an entity into its corresponding protocol buffer message representation.
   *
   * @param t The entity to be converted.
   * @param <T> The type of the entity.
   * @param <M> The type of the protocol buffer message.
   * @return The protocol buffer message representing the entity.
   * @throws IOException If there's an error during conversion or if no SerDe is found for the
   *     entity.
   */
  public <T extends Entity, M extends Message> M toProto(T t) throws IOException {
    if (!entityToSerDe.containsKey(t.getClass())) {
      throw new IOException("No SerDe found for entity " + t.getClass().getName());
    }

    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) entityToSerDe.get(t.getClass());
    return protoSerDe.serialize(t);
  }

  /**
   * Converts a protocol buffer message into its corresponding entity representation.
   *
   * @param m The protocol buffer message to be converted.
   * @param <T> The type of the entity.
   * @param <M> The type of the protocol buffer message.
   * @return The entity representing the protocol buffer message.
   * @throws IOException If there's an error during conversion or if no entity class or SerDe is
   *     found for the proto.
   */
  public <T extends Entity, M extends Message> T fromProto(M m) throws IOException {
    if (!protoToEntity.containsKey(m.getClass())) {
      throw new IOException("No entity class found for proto " + m.getClass().getName());
    }
    Class<? extends Entity> entityClass = protoToEntity.get(m.getClass());

    if (!entityToSerDe.containsKey(entityClass)) {
      throw new IOException("No SerDe found for entity " + entityClass.getName());
    }

    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) entityToSerDe.get(entityClass);
    return protoSerDe.deserialize(m);
  }

  /**
   * Loads a class using the specified class name and class loader.
   *
   * @param className The fully qualified name of the class to be loaded.
   * @param classLoader The class loader to use for loading the class.
   * @return The loaded class.
   * @throws IOException If there's an error while loading the class.
   */
  private Class<?> loadClass(String className, ClassLoader classLoader) throws IOException {
    try {
      return Class.forName(className, true, classLoader);
    } catch (Exception e) {
      throw new IOException("Failed to load class " + className, e);
    }
  }
}
