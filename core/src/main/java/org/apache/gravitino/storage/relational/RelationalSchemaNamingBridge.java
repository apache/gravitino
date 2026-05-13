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
import org.apache.gravitino.SupportsRelationOperations.Type;
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
import org.apache.gravitino.utils.MetadataObjectUtil;

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
  private static final String SENTINEL_METALAKE = "_";

  private RelationalSchemaNamingBridge() {}

  /**
   * Reconstructs a dotted metadata-object full name from a {@link NameIdentifier} that was created
   * by prepending {@link #SENTINEL_METALAKE} to the original full name. Skips the sentinel at
   * {@code namespace.levels()[0]} and joins the remaining levels with the identifier's name.
   */
  private static String identToMetadataObjectFullName(NameIdentifier ident) {
    String[] levels = ident.namespace().levels();
    List<String> parts = new ArrayList<>(levels.length);
    for (int i = 1; i < levels.length; i++) {
      parts.add(levels[i]);
    }
    parts.add(ident.name());
    return DOT_JOINER.join(parts);
  }

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
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(object.type());
    NameIdentifier ident = NameIdentifier.parse(SENTINEL_METALAKE + "." + object.fullName());
    NameIdentifier converted = nameIdentifierForStorage(ident, entityType);
    if (converted.equals(ident)) {
      return object;
    }
    return SecurableObjects.parse(
        identToMetadataObjectFullName(converted), object.type(), object.privileges());
  }

  /**
   * Converts the dotted metadata {@code fullName} on a {@link SecurableObject} to logical schema
   * naming for API callers. Passthrough rules match {@link
   * #securableObjectForStorage(SecurableObject)}.
   */
  public static SecurableObject securableObjectForApi(SecurableObject object) {
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(object.type());
    NameIdentifier ident = NameIdentifier.parse(SENTINEL_METALAKE + "." + object.fullName());
    NameIdentifier converted = nameIdentifierForApi(ident, entityType);
    if (converted.equals(ident)) {
      return object;
    }
    return SecurableObjects.parse(
        identToMetadataObjectFullName(converted), object.type(), object.privileges());
  }

  public static RoleEntity roleEntityForStorage(RoleEntity entity) {
    return mapRoleSecurableObjects(entity, RelationalSchemaNamingBridge::securableObjectForStorage);
  }

  public static RoleEntity roleEntityForApi(RoleEntity entity) {
    return mapRoleSecurableObjects(entity, RelationalSchemaNamingBridge::securableObjectForApi);
  }

  private static RoleEntity mapRoleSecurableObjects(
      RoleEntity entity, Function<SecurableObject, SecurableObject> mapper) {
    List<SecurableObject> objects = entity.securableObjects();
    if (objects == null || objects.isEmpty()) {
      return entity;
    }
    List<SecurableObject> mapped = objects.stream().map(mapper).collect(Collectors.toList());
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
   * <p>Only processes entity types that correspond to a {@link MetadataObject.Type} (e.g. TABLE,
   * SCHEMA). Types without a {@link MetadataObject.Type} equivalent (e.g. TABLE_STATISTIC,
   * MODEL_VERSION) are returned unchanged.
   */
  public static GenericEntity genericEntityMetadataFullNameForApi(GenericEntity entity) {
    NameIdentifier converted = nameIdentifierForStorage(entity.nameIdentifier(), entity.type());
    return GenericEntity.builder()
        .withId(entity.id())
        .withEntityType(entity.type())
        .withName(converted.name())
        .withNamespace(converted.namespace())
        .build();
  }

  /**
   * Converts the schema segment at namespace index 2 (the slot occupied by the schema name in the
   * standard {@code [metalake, catalog, schema, ...]} layout) from logical to physical form.
   * Callers must only pass namespaces that follow this layout; if the schema sits at a different
   * index the conversion will silently apply to the wrong segment.
   */
  public static Namespace embeddedNamespaceForStorage(Namespace ns) {
    if (ns.length() < 3) {
      return ns;
    }
    String[] lv = ns.levels();
    String[] copy = Arrays.copyOf(lv, lv.length);
    copy[2] = HierarchicalSchemaUtil.logicalToPhysical(copy[2], separator());
    return Namespace.of(copy);
  }

  /**
   * Converts the schema segment at namespace index 2 from physical to logical form. See {@link
   * #embeddedNamespaceForStorage(Namespace)} for the layout constraint.
   */
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
        // FILESET, TOPIC, MODEL, MODEL_VERSION and other types do not embed the schema
        // in their identifier path and are forwarded as-is to the meta services.
        return ident;
    }
  }

  /**
   * Converts identifiers from physical storage naming back to the logical form used by API callers.
   * Passthrough rules match {@link #nameIdentifierForStorage(NameIdentifier, Entity.EntityType)}.
   */
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
        // FILESET, TOPIC, MODEL, MODEL_VERSION and other types are returned unchanged.
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

  /**
   * Converts a {@link SchemaEntity}'s {@link SchemaEntity#name()} from logical (API) form to the
   * physical encoding used by relational meta services. Inverse of {@link #schemaEntityForApi}.
   */
  public static SchemaEntity schemaEntityForStorage(SchemaEntity entity) {
    String physicalName = HierarchicalSchemaUtil.logicalToPhysical(entity.name(), separator());
    if (physicalName.equals(entity.name())) {
      return entity;
    }
    return SchemaEntity.builder()
        .withId(entity.id())
        .withName(physicalName)
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

  /**
   * Converts a {@link StatisticEntity}'s namespace from physical storage form to logical API form.
   * Only {@link TableStatisticEntity} is currently stored with an embedded schema namespace; other
   * concrete subtypes are not expected and will cause an {@link IllegalArgumentException}.
   */
  public static StatisticEntity statisticEntityForApi(StatisticEntity e) {
    return statisticEntityWithNamespace(e, embeddedNamespaceForApi(e.namespace()));
  }

  /**
   * Converts a {@link StatisticEntity}'s namespace to the physical storage form. See {@link
   * #statisticEntityForApi(StatisticEntity)} for subtype restrictions.
   */
  public static StatisticEntity statisticEntityForStorage(StatisticEntity e) {
    return statisticEntityWithNamespace(e, embeddedNamespaceForStorage(e.namespace()));
  }

  private static StatisticEntity statisticEntityWithNamespace(StatisticEntity e, Namespace ns) {
    Preconditions.checkArgument(
        e instanceof TableStatisticEntity,
        "Only TableStatisticEntity is supported, got: %s",
        e.getClass().getSimpleName());
    if (ns.equals(e.namespace())) {
      return e;
    }
    return TableStatisticEntity.builder()
        .withId(e.id())
        .withName(e.name())
        .withValue(e.value())
        .withAuditInfo((AuditInfo) e.auditInfo())
        .withNamespace(ns)
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
  @SuppressWarnings("unchecked") // entityForApi/entityForStorage dispatch on known concrete types
  public static <E extends Entity & HasIdentifier> Function<E, E> wrapperUpdater(
      Entity.EntityType entityType, Function<E, E> updater) {
    return e -> {
      E logicalOld = entityForApi(e, entityType);
      E logicalNew = updater.apply(logicalOld);
      return entityForStorage(logicalNew);
    };
  }

  /**
   * Converts storage-shaped entities to API naming. {@link RoleEntity} and {@link GenericEntity}
   * (policy/tag relation list payloads) are normalized by concrete type before the {@code
   * type}-based branch so callers may pass each element's {@link Entity.EntityType} (e.g. roles
   * listed for a {@link Entity.EntityType#TABLE} metadata object still use {@code TABLE} while
   * elements are roles).
   */
  @SuppressWarnings(
      "unchecked") // each case casts to the statically known subtype for that EntityType
  public static <E extends Entity & HasIdentifier> E entityForApi(
      E entity, Entity.EntityType type) {
    if (entity instanceof GenericEntity) {
      return (E) genericEntityMetadataFullNameForApi((GenericEntity) entity);
    }

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
      case ROLE:
        return (E) roleEntityForApi((RoleEntity) entity);
      default:
        return entity;
    }
  }

  @SuppressWarnings("unchecked") // instanceof guards ensure each cast is safe
  public static <E extends Entity & HasIdentifier> E entityForStorage(E entity) {
    if (entity instanceof SchemaEntity) {
      return (E) schemaEntityForStorage((SchemaEntity) entity);
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
    if (entity instanceof RoleEntity) {
      return (E) roleEntityForStorage((RoleEntity) entity);
    }
    return entity;
  }

  /**
   * JDBC policy/tag relation APIs: a destination {@link NameIdentifier} is normalized using the
   * element type implied by {@code relType} when applicable (e.g. policy or tag name for
   * association rows).
   */
  public static NameIdentifier relationDestIdentifiersForStorage(
      Type relType, NameIdentifier ident) {
    Entity.EntityType elementType;
    if (relType == Type.POLICY_METADATA_OBJECT_REL) {
      elementType = Entity.EntityType.POLICY;
    } else if (relType == Type.TAG_METADATA_OBJECT_REL) {
      elementType = Entity.EntityType.TAG;
    } else {
      return ident;
    }
    return nameIdentifierForStorage(ident, elementType);
  }
}
