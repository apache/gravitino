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
package org.apache.gravitino.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.BaseSchema;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.PrincipalUtils;

public abstract class ManagedSchemaOperations implements SupportsSchemas {

  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema %s does not exist";

  public static class ManagedSchema extends BaseSchema {

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder extends BaseSchemaBuilder<Builder, ManagedSchema> {

      private Builder() {}

      @Override
      protected ManagedSchema internalBuild() {
        ManagedSchema schema = new ManagedSchema();
        schema.name = name;
        schema.comment = comment;
        schema.properties = properties;
        schema.auditInfo = auditInfo;
        return schema;
      }
    }
  }

  protected abstract EntityStore store();

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      List<SchemaEntity> schemas =
          store().list(namespace, SchemaEntity.class, Entity.EntityType.SCHEMA);
      return schemas.stream()
          .map(s -> NameIdentifier.of(namespace, s.name()))
          .toArray(NameIdentifier[]::new);

    } catch (NoSuchEntityException e) {
      throw new NoSuchCatalogException(e, "Catalog %s does not exist", namespace);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to list schemas under namespace " + namespace, ioe);
    }
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      if (store().exists(ident, Entity.EntityType.SCHEMA)) {
        throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident + " exists", ioe);
    }

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkArgument(stringId != null, "Property String identifier should not be null");

    SchemaEntity schemaEntity =
        SchemaEntity.builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withProperties(properties)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store().put(schemaEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create schema " + ident, ioe);
    } catch (NoSuchEntityException e) {
      throw new NoSuchCatalogException(e, "Catalog %s does not exist", ident.namespace());
    }

    return ManagedSchema.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(schemaEntity.properties())
        .withAuditInfo(schemaEntity.auditInfo())
        .build();
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      SchemaEntity schemaEntity = store().get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);

      return ManagedSchema.builder()
          .withName(ident.name())
          .withComment(schemaEntity.comment())
          .withProperties(schemaEntity.properties())
          .withAuditInfo(schemaEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + ident, ioe);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      SchemaEntity entity =
          store()
              .update(
                  ident,
                  SchemaEntity.class,
                  Entity.EntityType.SCHEMA,
                  schemaEntity -> updateSchemaEntity(ident, schemaEntity, changes));

      return ManagedSchema.builder()
          .withName(ident.name())
          .withComment(entity.comment())
          .withProperties(entity.properties())
          .withAuditInfo(entity.auditInfo())
          .build();

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to update schema " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchSchemaException(nsee, SCHEMA_DOES_NOT_EXIST_MSG, ident);
    } catch (AlreadyExistsException aee) {
      throw new RuntimeException(
          "Schema with the same name "
              + ident.name()
              + " already exists, this is unexpected because schema doesn't support rename",
          aee);
    }
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      return store().delete(ident, Entity.EntityType.SCHEMA, cascade);

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident, ioe);
    } catch (NonEmptyEntityException neee) {
      throw new NonEmptySchemaException(neee, "Schema %s is not empty", ident);
    } catch (NoSuchEntityException nsee) {
      return false;
    }
  }

  private SchemaEntity updateSchemaEntity(
      NameIdentifier ident, SchemaEntity schemaEntity, SchemaChange... changes) {
    Map<String, String> props =
        schemaEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(schemaEntity.properties());

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty setProperty = (SchemaChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        SchemaChange.RemoveProperty removeProperty = (SchemaChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else {
        throw new IllegalArgumentException(
            "Unsupported schema change: " + change.getClass().getSimpleName());
      }
    }

    return SchemaEntity.builder()
        .withName(schemaEntity.name())
        .withNamespace(ident.namespace())
        .withId(schemaEntity.id())
        .withComment(schemaEntity.comment())
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(schemaEntity.auditInfo().creator())
                .withCreateTime(schemaEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }
}
