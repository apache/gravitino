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
package com.apache.gravitino.proto;

import com.apache.gravitino.Entity;
import com.apache.gravitino.EntitySerDe;
import com.apache.gravitino.Namespace;
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
          .put("com.apache.gravitino.meta.AuditInfo", "com.apache.gravitino.proto.AuditInfoSerDe")
          .put(
              "com.apache.gravitino.meta.BaseMetalake",
              "com.apache.gravitino.proto.BaseMetalakeSerDe")
          .put(
              "com.apache.gravitino.meta.CatalogEntity",
              "com.apache.gravitino.proto.CatalogEntitySerDe")
          .put(
              "com.apache.gravitino.meta.SchemaEntity",
              "com.apache.gravitino.proto.SchemaEntitySerDe")
          .put(
              "com.apache.gravitino.meta.TableEntity",
              "com.apache.gravitino.proto.TableEntitySerDe")
          .put(
              "com.apache.gravitino.meta.FilesetEntity",
              "com.apache.gravitino.proto.FilesetEntitySerDe")
          .put(
              "com.apache.gravitino.meta.TopicEntity",
              "com.apache.gravitino.proto.TopicEntitySerDe")
          .put("com.apache.gravitino.meta.UserEntity", "com.apache.gravitino.proto.UserEntitySerDe")
          .put(
              "com.apache.gravitino.meta.GroupEntity",
              "com.apache.gravitino.proto.GroupEntitySerDe")
          .put("com.apache.gravitino.meta.RoleEntity", "com.apache.gravitino.proto.RoleEntitySerDe")
          .build();

  private static final Map<String, String> ENTITY_TO_PROTO =
      ImmutableMap.of(
          "com.apache.gravitino.meta.AuditInfo",
          "com.apache.gravitino.proto.AuditInfo",
          "com.apache.gravitino.meta.BaseMetalake",
          "com.apache.gravitino.proto.Metalake",
          "com.apache.gravitino.meta.CatalogEntity",
          "com.apache.gravitino.proto.Catalog",
          "com.apache.gravitino.meta.SchemaEntity",
          "com.apache.gravitino.proto.Schema",
          "com.apache.gravitino.meta.TableEntity",
          "com.apache.gravitino.proto.Table",
          "com.apache.gravitino.meta.FilesetEntity",
          "com.apache.gravitino.proto.Fileset",
          "com.apache.gravitino.meta.TopicEntity",
          "com.apache.gravitino.proto.Topic",
          "com.apache.gravitino.meta.UserEntity",
          "com.apache.gravitino.proto.User",
          "com.apache.gravitino.meta.GroupEntity",
          "com.apache.gravitino.proto.Group",
          "com.apache.gravitino.meta.RoleEntity",
          "com.apache.gravitino.proto.Role");

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
  public <T extends Entity> T deserialize(
      byte[] bytes, Class<T> clazz, ClassLoader classLoader, Namespace namespace)
      throws IOException {
    Any any = Any.parseFrom(bytes);
    Class<? extends Message> protoClass = getProtoClass(clazz, classLoader);

    if (!any.is(protoClass)) {
      throw new IOException("Invalid proto for entity " + clazz.getName());
    }

    Message anyMessage = any.unpack(protoClass);
    return fromProto(anyMessage, clazz, classLoader, namespace);
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
      M m, Class<T> entityClass, ClassLoader classLoader, Namespace namespace) throws IOException {
    ProtoSerDe<T, Message> protoSerDe = getProtoSerde(entityClass, classLoader);
    return protoSerDe.deserialize(m, namespace);
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
