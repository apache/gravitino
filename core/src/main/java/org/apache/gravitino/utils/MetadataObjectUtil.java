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
package org.apache.gravitino.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.IllegalMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchRoleException;

public class MetadataObjectUtil {

  private static final Joiner DOT = Joiner.on(".");

  private static final BiMap<MetadataObject.Type, Entity.EntityType> TYPE_TO_TYPE_MAP =
      ImmutableBiMap.<MetadataObject.Type, Entity.EntityType>builder()
          .put(MetadataObject.Type.METALAKE, Entity.EntityType.METALAKE)
          .put(MetadataObject.Type.CATALOG, Entity.EntityType.CATALOG)
          .put(MetadataObject.Type.SCHEMA, Entity.EntityType.SCHEMA)
          .put(MetadataObject.Type.TABLE, Entity.EntityType.TABLE)
          .put(MetadataObject.Type.TOPIC, Entity.EntityType.TOPIC)
          .put(MetadataObject.Type.FILESET, Entity.EntityType.FILESET)
          .put(MetadataObject.Type.COLUMN, Entity.EntityType.COLUMN)
          .put(MetadataObject.Type.ROLE, Entity.EntityType.ROLE)
          .put(MetadataObject.Type.MODEL, Entity.EntityType.MODEL)
          .build();

  private MetadataObjectUtil() {}

  /**
   * Map the given {@link MetadataObject}'s type to the corresponding {@link Entity.EntityType}.
   *
   * @param metadataObject The metadata object
   * @return The entity type
   * @throws IllegalArgumentException if the metadata object type is unknown
   */
  public static Entity.EntityType toEntityType(MetadataObject metadataObject) {
    Preconditions.checkArgument(metadataObject != null, "metadataObject cannot be null");

    return Optional.ofNullable(TYPE_TO_TYPE_MAP.get(metadataObject.type()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unknown metadata object type: " + metadataObject.type()));
  }

  /**
   * Map the given {@link MetadataObject}'s type to the corresponding {@link Entity.EntityType}.
   *
   * @param type The metadata object type
   * @return The entity type
   * @throws IllegalArgumentException if the metadata object type is unknown
   */
  public static Entity.EntityType toEntityType(MetadataObject.Type type) {
    Preconditions.checkArgument(type != null, "metadataObject type cannot be null");

    return Optional.ofNullable(TYPE_TO_TYPE_MAP.get(type))
        .orElseThrow(() -> new IllegalArgumentException("Unknown metadata object type: " + type));
  }

  /**
   * Convert the given {@link MetadataObject} full name to the corresponding {@link NameIdentifier}.
   *
   * @param metalakeName The metalake name
   * @param metadataObject The metadata object
   * @return The entity identifier
   * @throws IllegalArgumentException if the metadata object type is unsupported or unknown.
   */
  public static NameIdentifier toEntityIdent(String metalakeName, MetadataObject metadataObject) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName), "metalakeName cannot be blank");
    Preconditions.checkArgument(metadataObject != null, "metadataObject cannot be null");

    switch (metadataObject.type()) {
      case METALAKE:
        return NameIdentifierUtil.ofMetalake(metalakeName);
      case ROLE:
        return AuthorizationUtils.ofRole(metalakeName, metadataObject.name());
      case CATALOG:
      case SCHEMA:
      case TABLE:
      case TOPIC:
      case FILESET:
      case COLUMN:
      case MODEL:
        String fullName = DOT.join(metalakeName, metadataObject.fullName());
        return NameIdentifier.parse(fullName);
      default:
        throw new IllegalArgumentException(
            "Unknown metadata object type: " + metadataObject.type());
    }
  }

  /**
   * This method will check if the entity is existed explicitly, internally this check will load the
   * entity from underlying sources to entity store if not stored, and will allocate an uid for this
   * entity, with this uid tags can be associated with this entity. This method should be called out
   * of the tree lock, otherwise it will cause deadlock.
   *
   * @param metalake The metalake name
   * @param object The metadata object
   * @throws NoSuchMetadataObjectException if the metadata object type doesn't exist.
   */
  public static void checkMetadataObject(String metalake, MetadataObject object) {
    GravitinoEnv env = GravitinoEnv.getInstance();
    NameIdentifier identifier = toEntityIdent(metalake, object);

    Supplier<NoSuchMetadataObjectException> exceptionToThrowSupplier =
        () ->
            new NoSuchMetadataObjectException(
                "Metadata object %s type %s doesn't exist", object.fullName(), object.type());

    switch (object.type()) {
      case METALAKE:
        if (!metalake.equals(object.name())) {
          throw new IllegalMetadataObjectException("The metalake object name must be %s", metalake);
        }
        NameIdentifierUtil.checkMetalake(identifier);
        check(env.metalakeDispatcher().metalakeExists(identifier), exceptionToThrowSupplier);
        break;

      case CATALOG:
        NameIdentifierUtil.checkCatalog(identifier);
        check(env.catalogDispatcher().catalogExists(identifier), exceptionToThrowSupplier);
        break;

      case SCHEMA:
        NameIdentifierUtil.checkSchema(identifier);
        check(env.schemaDispatcher().schemaExists(identifier), exceptionToThrowSupplier);
        break;

      case FILESET:
        NameIdentifierUtil.checkFileset(identifier);
        check(env.filesetDispatcher().filesetExists(identifier), exceptionToThrowSupplier);
        break;

      case TABLE:
        NameIdentifierUtil.checkTable(identifier);
        check(env.tableDispatcher().tableExists(identifier), exceptionToThrowSupplier);
        break;

      case COLUMN:
        NameIdentifierUtil.checkColumn(identifier);
        NameIdentifier tableIdent = NameIdentifier.of(identifier.namespace().levels());
        check(env.tableDispatcher().tableExists(tableIdent), exceptionToThrowSupplier);
        break;

      case TOPIC:
        NameIdentifierUtil.checkTopic(identifier);
        check(env.topicDispatcher().topicExists(identifier), exceptionToThrowSupplier);
        break;

      case MODEL:
        NameIdentifierUtil.checkModel(identifier);
        check(env.modelDispatcher().modelExists(identifier), exceptionToThrowSupplier);
        break;

      case ROLE:
        AuthorizationUtils.checkRole(identifier);
        try {
          env.accessControlDispatcher().getRole(metalake, object.fullName());
        } catch (NoSuchRoleException nsr) {
          throw checkNotNull(exceptionToThrowSupplier).get();
        }
        break;

      default:
        throw new IllegalArgumentException(
            String.format("Doesn't support the type %s", object.type()));
    }
  }

  private static void check(
      final boolean expression, Supplier<? extends RuntimeException> exceptionToThrowSupplier) {
    if (!expression) {
      throw checkNotNull(exceptionToThrowSupplier).get();
    }
  }
}
