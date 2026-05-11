/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to
 * you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.storage.relational;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TableStatisticEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * Translates hierarchical schema naming between the logical representation used above {@link
 * JDBCBackend} (configured separator in identifiers/namespaces) and the physical representation
 * expected by relational meta services (ASCII-1 hierarchy separator in schema segments).
 *
 * <p>{@link Entity.EntityType#FILESET}, {@link Entity.EntityType#TOPIC}, {@link
 * Entity.EntityType#MODEL}, and {@link Entity.EntityType#MODEL_VERSION} identifiers/namespaces are
 * left unchanged by this bridge (delegated as-is to meta services).
 */
public final class RelationalSchemaNamingBridge {

  private static final Joiner DOT_JOINER = Joiner.on('.');

  private RelationalSchemaNamingBridge() {}

  private static String separator() {
    return HierarchicalSchemaUtil.schemaSeparator();
  }

  /**
   * Converts the dotted metadata {@code fullName} carried by {@link SecurableObject} so the schema
   * segment matches relational storage (physical hierarchical encoding). Types excluded from schema
   * embedding ({@link Entity.EntityType#FILESET}, {@link Entity.EntityType#TOPIC}, {@link
   * Entity.EntityType#MODEL}, {@link Entity.EntityType#MODEL_VERSION}) are unchanged.
   */
  public static SecurableObject securableObjectForStorage(SecurableObject object) {
    String converted =
        convertMetadataObjectDottedFullName(object.fullName(), object.type(), /* toStorage */ true);
    if (converted.equals(object.fullName())) {
      return object;
    }
    return SecurableObjects.parse(converted, object.type(), object.privileges());
  }

  /**
   * Converts the dotted metadata {@code fullName} on a {@link SecurableObject} to logical schema
   * naming for API callers. Passthrough rules match {@link
   * #securableObjectForStorage(SecurableObject)}.
   */
  public static SecurableObject securableObjectForApi(SecurableObject object) {
    String converted =
        convertMetadataObjectDottedFullName(
            object.fullName(), object.type(), /* toStorage */ false);
    if (converted.equals(object.fullName())) {
      return object;
    }
    return SecurableObjects.parse(converted, object.type(), object.privileges());
  }

  public static RoleEntity roleEntityForStorage(RoleEntity entity) {
    List<SecurableObject> objects = entity.securableObjects();
    if (objects == null || objects.isEmpty()) {
      return entity;
    }
    List<SecurableObject> mapped =
        objects.stream()
            .map(RelationalSchemaNamingBridge::securableObjectForStorage)
            .collect(Collectors.toList());
    if (mapped.equals(objects)) {
      return entity;
    }
    return RoleEntity.builder()
        .withId(entity.id())
        .withName(entity.name())
        .withNamespace(entity.namespace())
        .withProperties(entity.properties())
        .withAuditInfo(entity.auditInfo())
        .withSecurableObjects(mapped)
        .build();
  }

  public static RoleEntity roleEntityForApi(RoleEntity entity) {
    List<SecurableObject> objects = entity.securableObjects();
    if (objects == null || objects.isEmpty()) {
      return entity;
    }
    List<SecurableObject> mapped =
        objects.stream()
            .map(RelationalSchemaNamingBridge::securableObjectForApi)
            .collect(Collectors.toList());
    if (mapped.equals(objects)) {
      return entity;
    }
    return RoleEntity.builder()
        .withId(entity.id())
        .withName(entity.name())
        .withNamespace(entity.namespace())
        .withProperties(entity.properties())
        .withAuditInfo(entity.auditInfo())
        .withSecurableObjects(mapped)
        .build();
  }

  /**
   * Normalizes {@link GenericEntity#name()} when it holds a dotted metadata-object path whose
   * schema segment uses physical hierarchical encoding, for API callers above {@link JDBCBackend}.
   *
   * <p>Uses {@link MetadataObject.Type} derived from {@link Entity.EntityType} (same as relational
   * policy/tag stubs that only set dotted {@link GenericEntity#name()}, not a full table/view
   * namespace).
   */
  public static GenericEntity genericEntityMetadataFullNameForApi(GenericEntity entity) {
    String name = entity.name();
    if (name == null || name.isEmpty()) {
      return entity;
    }
    final MetadataObject.Type moType;
    try {
      moType = MetadataObject.Type.valueOf(entity.type().name());
    } catch (IllegalArgumentException e) {
      return entity;
    }
    String convertedName = convertMetadataObjectDottedFullName(name, moType, false);

    if (convertedName.equals(name)) {
      return entity;
    }
    return GenericEntity.builder()
        .withId(entity.id())
        .withEntityType(entity.type())
        .withName(convertedName)
        .withNamespace(entity.namespace())
        .build();
  }

  /**
   * Rewrites the schema segment inside a dotted metadata full name (catalog.schema[.rest]) between
   * logical and physical hierarchical forms. Non-matching part counts are returned unchanged.
   */
  static String convertMetadataObjectDottedFullName(
      String fullName, MetadataObject.Type type, boolean toStorage) {
    if (fullName == null || fullName.isEmpty()) {
      return fullName;
    }
    switch (type) {
      case SCHEMA:
        return convertSchemaSegmentAt(fullName, /* expectedParts */ 2, toStorage);
      case TABLE:
      case VIEW:
      case FUNCTION:
        return convertSchemaSegmentAt(fullName, /* expectedParts */ 3, toStorage);
      case COLUMN:
        return convertSchemaSegmentAt(fullName, /* expectedParts */ 4, toStorage);
      default:
        return fullName;
    }
  }

  private static String convertSchemaSegmentAt(
      String fullName, int expectedParts, boolean toStorage) {
    List<String> parts = SecurableObjects.DOT_SPLITTER.splitToList(fullName);
    if (parts.size() != expectedParts) {
      return fullName;
    }
    String schemaSegment = parts.get(1);
    if (schemaSegment == null || schemaSegment.isEmpty()) {
      return fullName;
    }
    String mapped =
        toStorage
            ? HierarchicalSchemaUtil.logicalToPhysical(schemaSegment, separator())
            : HierarchicalSchemaUtil.physicalToLogical(schemaSegment, separator());
    if (mapped.equals(schemaSegment)) {
      return fullName;
    }
    List<String> copy = new ArrayList<>(parts);
    copy.set(1, mapped);
    return DOT_JOINER.join(copy);
  }

  /** Converts the schema segment embedded at namespace index 2 when length ≥ 3. */
  public static Namespace embeddedNamespaceForStorage(Namespace ns) {
    if (ns.length() < 3) {
      return ns;
    }
    String[] lv = ns.levels();
    String[] copy = Arrays.copyOf(lv, lv.length);
    copy[2] = HierarchicalSchemaUtil.logicalToPhysical(copy[2], separator());
    return Namespace.of(copy);
  }

  public static Namespace embeddedNamespaceForApi(Namespace ns) {
    if (ns.length() < 3) {
      return ns;
    }
    String[] lv = ns.levels();
    String[] copy = Arrays.copyOf(lv, lv.length);
    copy[2] = HierarchicalSchemaUtil.physicalToLogical(copy[2], separator());
    return Namespace.of(copy);
  }

  public static NameIdentifier schemaIdentifierForStorage(NameIdentifier ident) {
    return NameIdentifier.of(
        ident.namespace(), HierarchicalSchemaUtil.logicalToPhysical(ident.name(), separator()));
  }

  public static NameIdentifier schemaIdentifierForApi(NameIdentifier ident) {
    return NameIdentifier.of(
        ident.namespace(), HierarchicalSchemaUtil.physicalToLogical(ident.name(), separator()));
  }

  /**
   * Converts identifiers carrying optional hierarchical schema segments before delegating to JDBC
   * meta services (physical naming).
   */
  public static NameIdentifier nameIdentifierForStorage(
      NameIdentifier ident, Entity.EntityType entityType) {
    switch (entityType) {
      case SCHEMA:
        return schemaIdentifierForStorage(ident);
      case TABLE:
      case VIEW:
      case FUNCTION:
      case COLUMN:
      case TABLE_STATISTIC:
        return NameIdentifier.of(embeddedNamespaceForStorage(ident.namespace()), ident.name());
      default:
        return ident;
    }
  }

  public static NameIdentifier nameIdentifierForApi(
      NameIdentifier ident, Entity.EntityType entityType) {
    switch (entityType) {
      case SCHEMA:
        return schemaIdentifierForApi(ident);
      case TABLE:
      case VIEW:
      case FUNCTION:
      case COLUMN:
      case TABLE_STATISTIC:
        return NameIdentifier.of(embeddedNamespaceForApi(ident.namespace()), ident.name());
      default:
        return ident;
    }
  }

  public static SchemaEntity schemaEntityForApi(SchemaEntity entity) {
    String logicalName = HierarchicalSchemaUtil.physicalToLogical(entity.name(), separator());
    if (logicalName.equals(entity.name())) {
      return entity;
    }
    return SchemaEntity.builder()
        .withId(entity.id())
        .withName(logicalName)
        .withNamespace(entity.namespace())
        .withComment(entity.comment())
        .withProperties(entity.properties())
        .withAuditInfo(entity.auditInfo())
        .build();
  }

  public static TableEntity tableEntityForApi(TableEntity e) {
    Namespace apiNs = embeddedNamespaceForApi(e.namespace());
    if (apiNs.equals(e.namespace())) {
      return e;
    }
    return TableEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withNamespace(apiNs)
        .withColumns(e.columns())
        .withAuditInfo(e.auditInfo())
        .withProperties(e.properties())
        .withPartitioning(e.partitioning())
        .withSortOrders(e.sortOrders())
        .withDistribution(e.distribution())
        .withIndexes(e.indexes())
        .withComment(e.comment())
        .build();
  }

  public static TableEntity tableEntityForStorage(TableEntity e) {
    Namespace storageNs = embeddedNamespaceForStorage(e.namespace());
    if (storageNs.equals(e.namespace())) {
      return e;
    }
    return TableEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withNamespace(storageNs)
        .withColumns(e.columns())
        .withAuditInfo(e.auditInfo())
        .withProperties(e.properties())
        .withPartitioning(e.partitioning())
        .withSortOrders(e.sortOrders())
        .withDistribution(e.distribution())
        .withIndexes(e.indexes())
        .withComment(e.comment())
        .build();
  }

  public static ViewEntity viewEntityForApi(ViewEntity e) {
    Namespace apiNs = embeddedNamespaceForApi(e.namespace());
    if (apiNs.equals(e.namespace())) {
      return e;
    }
    return ViewEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withNamespace(apiNs)
        .withColumns(e.columns())
        .withRepresentations(e.representations())
        .withDefaultCatalog(e.defaultCatalog())
        .withDefaultSchema(e.defaultSchema())
        .withComment(e.comment())
        .withProperties(e.properties())
        .withAuditInfo(e.auditInfo())
        .build();
  }

  public static ViewEntity viewEntityForStorage(ViewEntity e) {
    Namespace storageNs = embeddedNamespaceForStorage(e.namespace());
    if (storageNs.equals(e.namespace())) {
      return e;
    }
    return ViewEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withNamespace(storageNs)
        .withColumns(e.columns())
        .withRepresentations(e.representations())
        .withDefaultCatalog(e.defaultCatalog())
        .withDefaultSchema(e.defaultSchema())
        .withComment(e.comment())
        .withProperties(e.properties())
        .withAuditInfo(e.auditInfo())
        .build();
  }

  public static FunctionEntity functionEntityForApi(FunctionEntity e) {
    Namespace apiNs = embeddedNamespaceForApi(e.namespace());
    if (apiNs.equals(e.namespace())) {
      return e;
    }
    return FunctionEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withNamespace(apiNs)
        .withComment(e.comment())
        .withFunctionType(e.functionType())
        .withDeterministic(e.deterministic())
        .withDefinitions(e.definitions())
        .withAuditInfo(e.auditInfo())
        .build();
  }

  public static FunctionEntity functionEntityForStorage(FunctionEntity e) {
    Namespace storageNs = embeddedNamespaceForStorage(e.namespace());
    if (storageNs.equals(e.namespace())) {
      return e;
    }
    return FunctionEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withNamespace(storageNs)
        .withComment(e.comment())
        .withFunctionType(e.functionType())
        .withDeterministic(e.deterministic())
        .withDefinitions(e.definitions())
        .withAuditInfo(e.auditInfo())
        .build();
  }

  public static StatisticEntity statisticEntityForApi(StatisticEntity e) {
    Namespace apiNs = embeddedNamespaceForApi(e.namespace());
    if (apiNs.equals(e.namespace())) {
      return e;
    }
    return TableStatisticEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withValue(e.value())
        .withAuditInfo((AuditInfo) e.auditInfo())
        .withNamespace(apiNs)
        .build();
  }

  public static StatisticEntity statisticEntityForStorage(StatisticEntity e) {
    Namespace storageNs = embeddedNamespaceForStorage(e.namespace());
    if (storageNs.equals(e.namespace())) {
      return e;
    }
    return TableStatisticEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withValue(e.value())
        .withAuditInfo((AuditInfo) e.auditInfo())
        .withNamespace(storageNs)
        .build();
  }

  /**
   * Wraps a meta-service {@code updater} used from {@link JDBCBackend}: converts entities from
   * storage naming to API naming, applies {@code updater}, then converts back to storage for
   * relational persistence.
   *
   * @param entityType bridged entity kind (e.g. {@link Entity.EntityType#TABLE}); passed to {@link
   *     #entityForApi(Entity, Entity.EntityType)}
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> Function<E, E> wrapperUpdater(
      Entity.EntityType entityType, Function<E, E> updater) {
    Preconditions.checkArgument(
        entityType != Entity.EntityType.SCHEMA,
        "Schema entity updates must go through SchemaMetaService directly, not wrapperUpdater");
    return e -> {
      E logicalOld = entityForApi(e, entityType);
      E logicalNew = updater.apply(logicalOld);
      return entityForStorage(logicalNew);
    };
  }

  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> E entityForApi(
      E entity, Entity.EntityType type) {
    switch (type) {
      case SCHEMA:
        return (E) schemaEntityForApi((SchemaEntity) entity);
      case TABLE:
        return (E) tableEntityForApi((TableEntity) entity);
      case VIEW:
        return (E) viewEntityForApi((ViewEntity) entity);
      case FUNCTION:
        return (E) functionEntityForApi((FunctionEntity) entity);
      case TABLE_STATISTIC:
        return (E) statisticEntityForApi((StatisticEntity) entity);
      default:
        return entity;
    }
  }

  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> E entityForStorage(E entity) {
    if (entity instanceof SchemaEntity) {
      return entity;
    }
    if (entity instanceof TableEntity) {
      return (E) tableEntityForStorage((TableEntity) entity);
    }
    if (entity instanceof ViewEntity) {
      return (E) viewEntityForStorage((ViewEntity) entity);
    }
    if (entity instanceof FunctionEntity) {
      return (E) functionEntityForStorage((FunctionEntity) entity);
    }
    if (entity instanceof StatisticEntity) {
      return (E) statisticEntityForStorage((StatisticEntity) entity);
    }
    return entity;
  }
}
