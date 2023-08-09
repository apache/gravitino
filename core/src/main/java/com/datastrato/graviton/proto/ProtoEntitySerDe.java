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

public class ProtoEntitySerDe implements EntitySerDe {

  // The implementation of different entities should also register its class to this map,
  // otherwise ProtoEntitySerDe will not be able to deserialize the entity.
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

  public ProtoEntitySerDe() {
    this.entityToSerDe = Maps.newHashMap();
    this.entityToProto = Maps.newHashMap();
  }

  @Override
  public <T extends Entity> byte[] serialize(T t) throws IOException {
    Any any = Any.pack(toProto(t));
    return any.toByteArray();
  }

  @Override
  public <T extends Entity> T deserialize(byte[] bytes, Class<T> clazz, ClassLoader classLoader)
      throws IOException {
    Any any = Any.parseFrom(bytes);

    if (!ENTITY_TO_SERDE.containsKey(clazz.getCanonicalName())
        || !ENTITY_TO_PROTO.containsKey(clazz.getCanonicalName())) {
      throw new IOException("No proto and serde class found for entity " + clazz.getName());
    }

    Class<? extends Message> protoClass =
        entityToProto.computeIfAbsent(
            clazz,
            k -> {
              try {
                return (Class<? extends Message>)
                    loadClass(
                        ENTITY_TO_PROTO.get(k.getCanonicalName()), getClass().getClassLoader());
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to create proto class " + k.getCanonicalName(), e);
              }
            });

    if (!any.is(protoClass)) {
      throw new IOException("Invalid proto for entity " + clazz.getName());
    }

    ProtoSerDe<T, Message> protoSerDe =
        (ProtoSerDe<T, Message>)
            entityToSerDe.computeIfAbsent(
                clazz,
                entityClazz -> {
                  try {
                    Class<? extends ProtoSerDe<? extends Entity, ? extends Message>> serdeClazz =
                        (Class<? extends ProtoSerDe<? extends Entity, ? extends Message>>)
                            loadClass(
                                ENTITY_TO_SERDE.get(entityClazz.getCanonicalName()),
                                clazz.getClass().getClassLoader());
                    return serdeClazz.newInstance();
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Failed to instantiate serde class " + entityClazz.getCanonicalName(), e);
                  }
                });
    Message anyMessage = any.unpack(protoClass);
    return protoSerDe.deserialize(anyMessage);
  }

  private <T extends Entity, M extends Message> M toProto(T t) throws IOException {
    if (!ENTITY_TO_SERDE.containsKey(t.getClass().getCanonicalName())
        || ENTITY_TO_SERDE.get(t.getClass().getCanonicalName()) == null) {
      throw new IOException("No serde found for entity " + t.getClass().getName());
    }

    ProtoSerDe<T, M> protoSerDe =
        (ProtoSerDe<T, M>)
            entityToSerDe.computeIfAbsent(
                t.getClass(),
                entityClazz -> {
                  try {
                    Class<? extends ProtoSerDe<? extends Entity, ? extends Message>> serdeClazz =
                        (Class<? extends ProtoSerDe<? extends Entity, ? extends Message>>)
                            loadClass(
                                ENTITY_TO_SERDE.get(t.getClass().getCanonicalName()),
                                t.getClass().getClassLoader());
                    return serdeClazz.newInstance();
                  } catch (Exception e) {
                    throw new RuntimeException(
                        "Failed to instantiate serde class "
                            + ENTITY_TO_SERDE.get(t.getClass().getCanonicalName()),
                        e);
                  }
                });
    return protoSerDe.serialize(t);
  }

  private Class<?> loadClass(String className, ClassLoader classLoader) throws IOException {
    try {
      return Class.forName(className, true, classLoader);
    } catch (Exception e) {
      throw new IOException("Failed to load class " + className, e);
    }
  }
}
