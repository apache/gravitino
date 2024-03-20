/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntitySerDe;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Map;

public class ProtoEntitySerDe implements EntitySerDe {

  // The implementation of different entities should also register its class to this map,
  // otherwise ProtoEntitySerDe will not be able to deserialize the entity.
  private static final Map<String, String> ENTITY_TO_SERDE =
      ImmutableMap.<String, String>builder()
          .put(
              "com.datastrato.gravitino.meta.AuditInfo",
              "com.datastrato.gravitino.proto.AuditInfoSerDe")
          .put(
              "com.datastrato.gravitino.meta.BaseMetalake",
              "com.datastrato.gravitino.proto.BaseMetalakeSerDe")
          .put(
              "com.datastrato.gravitino.meta.CatalogEntity",
              "com.datastrato.gravitino.proto.CatalogEntitySerDe")
          .put(
              "com.datastrato.gravitino.meta.SchemaEntity",
              "com.datastrato.gravitino.proto.SchemaEntitySerDe")
          .put(
              "com.datastrato.gravitino.meta.TableEntity",
              "com.datastrato.gravitino.proto.TableEntitySerDe")
          .put(
              "com.datastrato.gravitino.meta.FilesetEntity",
              "com.datastrato.gravitino.proto.FilesetEntitySerDe")
          .put(
              "com.datastrato.gravitino.meta.TopicEntity",
              "com.datastrato.gravitino.proto.TopicEntitySerDe")
          .build();

  private static final Map<String, String> ENTITY_TO_PROTO =
      ImmutableMap.of(
          "com.datastrato.gravitino.meta.AuditInfo",
          "com.datastrato.gravitino.proto.AuditInfo",
          "com.datastrato.gravitino.meta.BaseMetalake",
          "com.datastrato.gravitino.proto.Metalake",
          "com.datastrato.gravitino.meta.CatalogEntity",
          "com.datastrato.gravitino.proto.Catalog",
          "com.datastrato.gravitino.meta.SchemaEntity",
          "com.datastrato.gravitino.proto.Schema",
          "com.datastrato.gravitino.meta.TableEntity",
          "com.datastrato.gravitino.proto.Table",
          "com.datastrato.gravitino.meta.FilesetEntity",
          "com.datastrato.gravitino.proto.Fileset",
          "com.datastrato.gravitino.meta.TopicEntity",
          "com.datastrato.gravitino.proto.Topic");

  private final Map<Class<? extends Entity>, ProtoSerDe<? extends Entity, ? extends Message>>
      entityToSerDe;

  private final Map<Class<? extends Entity>, Class<? extends Message>> entityToProto;

  public ProtoEntitySerDe() {
    this.entityToSerDe = Maps.newConcurrentMap();
    this.entityToProto = Maps.newConcurrentMap();
  }

  @Override
  public <T extends Entity> byte[] serialize(T t) throws IOException {
    Any any = Any.pack(toProto(t, Thread.currentThread().getContextClassLoader()));
    return any.toByteArray();
  }

  @Override
  public <T extends Entity> T deserialize(byte[] bytes, Class<T> clazz, ClassLoader classLoader)
      throws IOException {
    Any any = Any.parseFrom(bytes);
    Class<? extends Message> protoClass = getProtoClass(clazz, classLoader);

    if (!any.is(protoClass)) {
      throw new IOException("Invalid proto for entity " + clazz.getName());
    }

    Message anyMessage = any.unpack(protoClass);
    return fromProto(anyMessage, clazz, classLoader);
  }

  private <T extends Entity, M extends Message> ProtoSerDe<T, M> getProtoSerde(
      Class<T> entityClass, ClassLoader classLoader) throws IOException {
    if (!ENTITY_TO_SERDE.containsKey(entityClass.getCanonicalName())
        || ENTITY_TO_SERDE.get(entityClass.getCanonicalName()) == null) {
      throw new IOException("No serde found for entity " + entityClass.getCanonicalName());
    }
    return (ProtoSerDe<T, M>)
        entityToSerDe.computeIfAbsent(
            entityClass,
            k -> {
              try {
                Class<? extends ProtoSerDe<? extends Entity, ? extends Message>> serdeClazz =
                    (Class<? extends ProtoSerDe<? extends Entity, ? extends Message>>)
                        loadClass(ENTITY_TO_SERDE.get(k.getCanonicalName()), classLoader);
                return serdeClazz.getDeclaredConstructor().newInstance();
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to instantiate serde class " + k.getCanonicalName(), e);
              }
            });
  }

  private Class<? extends Message> getProtoClass(
      Class<? extends Entity> entityClass, ClassLoader classLoader) throws IOException {
    if (!ENTITY_TO_PROTO.containsKey(entityClass.getCanonicalName())
        || ENTITY_TO_PROTO.get(entityClass.getCanonicalName()) == null) {
      throw new IOException("No proto class found for entity " + entityClass.getCanonicalName());
    }
    return entityToProto.computeIfAbsent(
        entityClass,
        k -> {
          try {
            return (Class<? extends Message>)
                loadClass(ENTITY_TO_PROTO.get(k.getCanonicalName()), classLoader);
          } catch (Exception e) {
            throw new RuntimeException("Failed to create proto class " + k.getCanonicalName(), e);
          }
        });
  }

  private <T extends Entity, M extends Message> M toProto(T t, ClassLoader classLoader)
      throws IOException {
    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) getProtoSerde(t.getClass(), classLoader);
    return protoSerDe.serialize(t);
  }

  private <T extends Entity, M extends Message> T fromProto(
      M m, Class<T> entityClass, ClassLoader classLoader) throws IOException {
    ProtoSerDe<T, Message> protoSerDe = getProtoSerde(entityClass, classLoader);
    return protoSerDe.deserialize(m);
  }

  private Class<?> loadClass(String className, ClassLoader classLoader) throws IOException {
    try {
      return Class.forName(className, true, classLoader);
    } catch (Exception e) {
      throw new IOException(
          "Failed to load class " + className + " with classLoader " + classLoader, e);
    }
  }
}
