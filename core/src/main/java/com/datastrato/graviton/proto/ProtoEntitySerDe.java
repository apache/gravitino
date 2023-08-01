/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.proto;

import static com.datastrato.graviton.util.IsolatedClassLoader.loadClass;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.EntitySerDe;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

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

  private final Map<Class<? extends Message>, Class<? extends Entity>> protoToEntity;

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

  @Override
  public <T extends Entity> byte[] serialize(T t) throws IOException {
    Any any = Any.pack(toProto(t));
    return any.toByteArray();
  }

  @Override
  public <T extends Entity> T deserialize(byte[] bytes, Class<T> clazz, ClassLoader classLoader)
      throws IOException {
    Any any = Any.parseFrom(bytes);

    if (!entityToSerDe.containsKey(clazz) || !entityToProto.containsKey(clazz)) {
      throw new IOException("No proto and serde class found for entity " + clazz.getName());
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

  public <T extends Entity, M extends Message> M toProto(T t) throws IOException {
    if (!entityToSerDe.containsKey(t.getClass())) {
      throw new IOException("No serde found for entity " + t.getClass().getName());
    }

    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) entityToSerDe.get(t.getClass());
    return protoSerDe.serialize(t);
  }

  public <T extends Entity, M extends Message> T fromProto(M m) throws IOException {
    if (!protoToEntity.containsKey(m.getClass())) {
      throw new IOException("No entity class found for proto " + m.getClass().getName());
    }
    Class<? extends Entity> entityClass = protoToEntity.get(m.getClass());

    if (!entityToSerDe.containsKey(entityClass)) {
      throw new IOException("No serde found for entity " + entityClass.getName());
    }

    ProtoSerDe<T, M> protoSerDe = (ProtoSerDe<T, M>) entityToSerDe.get(entityClass);
    return protoSerDe.deserialize(m);
  }
}
