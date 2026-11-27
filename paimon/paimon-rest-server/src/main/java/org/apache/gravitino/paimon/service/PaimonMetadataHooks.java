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
package org.apache.gravitino.paimon.service;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;

/** Keeps ownership and registered metadata consistent with successful auxiliary REST mutations. */
public class PaimonMetadataHooks {
  private final boolean enabled;
  private final String metalake;

  /**
   * Creates hooks for a deployment mode.
   *
   * @param enabled whether auxiliary metadata synchronization is enabled
   * @param metalake registered catalogs' metalake
   */
  public PaimonMetadataHooks(boolean enabled, String metalake) {
    this.enabled = enabled;
    this.metalake = metalake;
  }

  /** The metadata effects of an already authorized native mutation. */
  public enum Change {
    /** Create a schema and assign its owner. */
    CREATE_SCHEMA,
    /** Create a table and assign its owner. */
    CREATE_TABLE,
    /** Refresh registered schema metadata. */
    ALTER_SCHEMA,
    /** Refresh registered table metadata. */
    ALTER_TABLE,
    /** Remove registered schema metadata after dropping the backend schema. */
    DROP_SCHEMA,
    /** Remove registered table metadata after dropping the backend table. */
    DROP_TABLE
  }

  /**
   * Runs a native mutation under the same tree lock used by Gravitino and updates its metadata.
   *
   * @param <T> native result type
   * @return native operation result
   * @param change the operation's metadata effect
   * @param identifier full Gravitino identifier including metalake
   * @param mutation native Paimon operation
   */
  public <T> T mutate(Change change, NameIdentifier identifier, Supplier<T> mutation) {
    if (!enabled) {
      return mutation.get();
    }
    boolean schema =
        change == Change.CREATE_SCHEMA
            || change == Change.ALTER_SCHEMA
            || change == Change.DROP_SCHEMA;
    Entity.EntityType type = schema ? Entity.EntityType.SCHEMA : Entity.EntityType.TABLE;
    NameIdentifierUtil.toMetadataObject(identifier, type);
    if (change == Change.CREATE_TABLE) {
      TableEntity.NAME.validate(identifier.name(), Entity.EntityType.TABLE);
    } else if (change == Change.CREATE_SCHEMA) {
      EntityFieldLimits.checkMaxLength(
          identifier.name(), EntityFieldLimits.MAX_NAME_LENGTH, "name", Entity.EntityType.SCHEMA);
    }
    if (!schema) {
      NameIdentifierUtil.toMetadataObject(
          NameIdentifier.of(identifier.namespace().levels()), Entity.EntityType.SCHEMA);
    }
    // Importing a cold schema needs its parent catalog's WRITE lock. Acquire that ancestor
    // first, rather than holding a descendant lock and attempting an unsupported lock upgrade.
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(metalake, identifier.namespace().level(1)),
        LockType.WRITE,
        () -> {
          boolean dropped = change == Change.DROP_SCHEMA || change == Change.DROP_TABLE;
          List<String> locations =
              dropped ? AuthorizationUtils.getMetadataObjectLocation(identifier, type) : null;
          if (change == Change.CREATE_TABLE || change == Change.CREATE_SCHEMA) {
            requireUnregistered(identifier, type);
          }
          T result = mutation.get();
          if (dropped) {
            delete(identifier, type);
            AuthorizationUtils.authorizationPluginRemovePrivileges(identifier, type, locations);
          } else {
            boolean created = change == Change.CREATE_SCHEMA || change == Change.CREATE_TABLE;
            AuditInfo audit =
                created
                    ? AuditInfo.builder()
                        .withCreator(PrincipalUtils.getCurrentUserName())
                        .withCreateTime(Instant.now())
                        .build()
                    : AuditInfo.EMPTY;
            if (schema) {
              GravitinoEnv.getInstance().internalSchemaDispatcher().importSchema(identifier, audit);
            } else {
              GravitinoEnv.getInstance().internalTableDispatcher().importTable(identifier, audit);
            }
            if (created && GravitinoEnv.getInstance().internalOwnerDispatcher() != null) {
              GravitinoEnv.getInstance()
                  .internalOwnerDispatcher()
                  .setOwner(
                      metalake,
                      NameIdentifierUtil.toMetadataObject(identifier, type),
                      PrincipalUtils.getCurrentUserName(),
                      Owner.Type.USER);
            }
          }
          return result;
        });
  }

  /**
   * Preserves a table's identity and ownership during a native rename.
   *
   * @param source old Gravitino identifier
   * @param destination new Gravitino identifier
   * @param mutation native rename
   */
  public void rename(NameIdentifier source, NameIdentifier destination, Runnable mutation) {
    if (!enabled) {
      mutation.run();
      return;
    }
    NameIdentifierUtil.toMetadataObject(source, Entity.EntityType.TABLE);
    NameIdentifierUtil.toMetadataObject(destination, Entity.EntityType.TABLE);
    TableEntity.NAME.validate(destination.name(), Entity.EntityType.TABLE);
    TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(metalake, source.namespace().level(1)),
        LockType.WRITE,
        () -> {
          // Import an external source before rename so its existing registration can be moved.
          importSourceTable(source);
          try {
            GravitinoEnv.getInstance()
                .internalSchemaDispatcher()
                .importSchema(NameIdentifier.of(destination.namespace().levels()), AuditInfo.EMPTY);
          } catch (NoSuchSchemaException e) {
            throw new NoSuchResourceException(
                "DATABASE",
                destination.namespace().level(2),
                "Destination database does not exist.");
          }
          if (!source.equals(destination)) {
            requireUnregistered(destination, Entity.EntityType.TABLE);
          }
          List<String> locations =
              AuthorizationUtils.getMetadataObjectLocation(source, Entity.EntityType.TABLE);
          mutation.run();
          try {
            GravitinoEnv.getInstance()
                .entityStore()
                .update(
                    source,
                    TableEntity.class,
                    Entity.EntityType.TABLE,
                    table ->
                        TableEntity.builder()
                            .withId(table.id())
                            .withName(destination.name())
                            .withNamespace(destination.namespace())
                            .withColumns(table.columns())
                            .withProperties(table.properties())
                            .withPartitioning(table.partitioning())
                            .withSortOrders(table.sortOrders())
                            .withDistribution(table.distribution())
                            .withIndexes(table.indexes())
                            .withComment(table.comment())
                            .withAuditInfo(
                                AuditInfo.builder()
                                    .withCreator(table.auditInfo().creator())
                                    .withCreateTime(table.auditInfo().createTime())
                                    .withLastModifier(PrincipalUtils.getCurrentUserName())
                                    .withLastModifiedTime(Instant.now())
                                    .build())
                            .build());
          } catch (IOException e) {
            throw new IllegalStateException("Could not update renamed table metadata", e);
          }
          AuthorizationUtils.authorizationPluginRenamePrivileges(
              source, Entity.EntityType.TABLE, destination, locations);
          return null;
        });
  }

  /**
   * Refreshes a native schema change while retaining column identities from its field IDs.
   *
   * @param identifier registered table identifier
   * @param fields loads the native top-level fields before or after the change
   * @param mutation applies the native schema change
   */
  public void alterTable(
      NameIdentifier identifier, Supplier<List<DataField>> fields, Runnable mutation) {
    if (!enabled) {
      mutation.run();
      return;
    }
    NameIdentifierUtil.toMetadataObject(identifier, Entity.EntityType.TABLE);
    TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(metalake, identifier.namespace().level(1)),
        LockType.WRITE,
        () -> {
          // Register the old columns before changing the native schema, including cold tables.
          importSourceTable(identifier);
          List<DataField> before = fields.get();
          mutation.run();
          Map<Integer, String> after = new HashMap<>();
          for (DataField field : fields.get()) {
            after.put(field.id(), field.name());
          }
          Map<String, String> names = new HashMap<>();
          for (DataField field : before) {
            String name = after.get(field.id());
            if (!field.name().equals(name)) {
              names.put(field.name(), name);
            }
          }
          GravitinoEnv.getInstance()
              .internalTableDispatcher()
              .importTable(identifier, AuditInfo.EMPTY, names);
          return null;
        });
  }

  /**
   * Validates a creation schema before the native table is written.
   *
   * @param schema the requested schema
   */
  public void validateCreateTable(Schema schema) {
    schema.options().keySet().forEach(PaimonMetadataHooks::validateMutableProperty);
    if (enabled) {
      for (DataField field : schema.fields()) {
        validateColumn(field.name(), field.description());
      }
    }
  }

  /**
   * Validates schema changes before any native change is applied.
   *
   * @param changes requested schema changes
   */
  public void validateTableChanges(List<SchemaChange> changes) {
    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetOption) {
        validateMutableProperty(((SchemaChange.SetOption) change).key());
      } else if (change instanceof SchemaChange.RemoveOption) {
        validateMutableProperty(((SchemaChange.RemoveOption) change).key());
      }
      if (!enabled) {
        continue;
      }
      if (change instanceof SchemaChange.AddColumn) {
        SchemaChange.AddColumn added = (SchemaChange.AddColumn) change;
        if (added.fieldNames().length == 1) {
          validateColumn(added.fieldNames()[0], added.description());
        }
      } else if (change instanceof SchemaChange.RenameColumn) {
        SchemaChange.RenameColumn renamed = (SchemaChange.RenameColumn) change;
        if (renamed.fieldNames().length == 1) {
          ColumnEntity.NAME.validate(renamed.newName(), Entity.EntityType.COLUMN);
        }
      } else if (change instanceof SchemaChange.UpdateColumnComment) {
        SchemaChange.UpdateColumnComment comment = (SchemaChange.UpdateColumnComment) change;
        if (comment.fieldNames().length == 1) {
          ColumnEntity.validateCommentForWrite(comment.fieldNames()[0], comment.newDescription());
        }
      }
    }
  }

  private static void validateMutableProperty(String key) {
    if (StringIdentifier.ID_KEY.equals(key)) {
      throw new IllegalArgumentException("Table identity property is reserved: " + key);
    }
  }

  private static void validateColumn(String name, @Nullable String comment) {
    ColumnEntity.NAME.validate(name, Entity.EntityType.COLUMN);
    ColumnEntity.validateCommentForWrite(name, comment);
  }

  private void requireUnregistered(NameIdentifier identifier, Entity.EntityType type) {
    try {
      if (GravitinoEnv.getInstance().entityStore().exists(identifier, type)) {
        throw new AlreadyExistsException(
            type == Entity.EntityType.SCHEMA ? "DATABASE" : type.name(),
            identifier.name(),
            "Resource is already registered in Gravitino.");
      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not check table registration", e);
    }
  }

  private void importSourceTable(NameIdentifier source) {
    try {
      GravitinoEnv.getInstance().internalTableDispatcher().importTable(source, AuditInfo.EMPTY);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchResourceException(
          "DATABASE", source.namespace().level(2), "Source database does not exist.");
    } catch (NoSuchTableException e) {
      throw new NoSuchResourceException("TABLE", source.name(), "Source table does not exist.");
    }
  }

  private void delete(NameIdentifier identifier, Entity.EntityType type) {
    try {
      GravitinoEnv.getInstance()
          .entityStore()
          .delete(identifier, type, type == Entity.EntityType.SCHEMA);
    } catch (NoSuchEntityException e) {
      /* The external entity may never have been imported. */
    } catch (IOException e) {
      throw new IllegalStateException("Could not remove dropped metadata", e);
    }
  }
}
