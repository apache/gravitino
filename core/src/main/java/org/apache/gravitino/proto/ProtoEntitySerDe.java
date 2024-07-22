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
package org.apache.gravitino.proto;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntitySerDe;
import org.apache.gravitino.Namespace;

public class ProtoEntitySerDe implements EntitySerDe {

  // The implementation of different entities should also register its class to this map,
  // otherwise ProtoEntitySerDe will not be able to deserialize the entity.
  private static final Map<String, String> ENTITY_TO_SERDE =
      ImmutableMap.<String, String>builder()
          .put("org.apache.gravitino.meta.AuditInfo", "org.apache.gravitino.proto.AuditInfoSerDe")
          .put(
              "org.apache.gravitino.meta.BaseMetalake",
              "org.apache.gravitino.proto.BaseMetalakeSerDe")
          .put(
              "org.apache.gravitino.meta.CatalogEntity",
              "org.apache.gravitino.proto.CatalogEntitySerDe")
          .put(
              "org.apache.gravitino.meta.SchemaEntity",
              "org.apache.gravitino.proto.SchemaEntitySerDe")
          .put(
              "org.apache.gravitino.meta.TableEntity",
              "org.apache.gravitino.proto.TableEntitySerDe")
          .put(
              "org.apache.gravitino.meta.FilesetEntity",
              "org.apache.gravitino.proto.FilesetEntitySerDe")
          .put(
              "org.apache.gravitino.meta.TopicEntity",
              "org.apache.gravitino.proto.TopicEntitySerDe")
          .put("org.apache.gravitino.meta.UserEntity", "org.apache.gravitino.proto.UserEntitySerDe")
          .put(
              "org.apache.gravitino.meta.GroupEntity",
              "org.apache.gravitino.proto.GroupEntitySerDe")
          .put("org.apache.gravitino.meta.RoleEntity", "org.apache.gravitino.proto.RoleEntitySerDe")
          .build();

  private static final Map<String, String> ENTITY_TO_PROTO =
      ImmutableMap.of(
          "org.apache.gravitino.meta.AuditInfo",
          "org.apache.gravitino.proto.AuditInfo",
          "org.apache.gravitino.meta.BaseMetalake",
          "org.apache.gravitino.proto.Metalake",
          "org.apache.gravitino.meta.CatalogEntity",
          "org.apache.gravitino.proto.Catalog",
          "org.apache.gravitino.meta.SchemaEntity",
          "org.apache.gravitino.proto.Schema",
          "org.apache.gravitino.meta.TableEntity",
          "org.apache.gravitino.proto.Table",
          "org.apache.gravitino.meta.FilesetEntity",
          "org.apache.gravitino.proto.Fileset",
          "org.apache.gravitino.meta.TopicEntity",
          "org.apache.gravitino.proto.Topic",
          "org.apache.gravitino.meta.UserEntity",
          "org.apache.gravitino.proto.User",
          "org.apache.gravitino.meta.GroupEntity",
          "org.apache.gravitino.proto.Group",
          "org.apache.gravitino.meta.RoleEntity",
          "org.apache.gravitino.proto.Role");

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
