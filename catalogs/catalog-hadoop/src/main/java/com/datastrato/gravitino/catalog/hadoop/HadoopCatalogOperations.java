/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import static com.datastrato.gravitino.catalog.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.CatalogOperations;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.PrincipalUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCatalogOperations implements CatalogOperations, SupportsSchemas, FilesetCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopCatalogOperations.class);

  private static final HadoopCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new HadoopCatalogPropertiesMetadata();

  private static final HadoopSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new HadoopSchemaPropertiesMetadata();

  private static final HadoopFilesetPropertiesMetadata FILESET_PROPERTIES_METADATA =
      new HadoopFilesetPropertiesMetadata();

  private final CatalogEntity entity;

  private final EntityStore store;

  private final IdGenerator idGenerator;

  @VisibleForTesting Configuration hadoopConf;

  @VisibleForTesting Optional<Path> catalogStorageLocation;

  // For testing only.
  HadoopCatalogOperations(CatalogEntity entity, EntityStore store, IdGenerator idGenerator) {
    this.entity = entity;
    this.store = store;
    this.idGenerator = idGenerator;
  }

  public HadoopCatalogOperations(CatalogEntity entity) {
    this(
        entity, GravitinoEnv.getInstance().entityStore(), GravitinoEnv.getInstance().idGenerator());
  }

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {
    // Initialize Hadoop Configuration.
    this.hadoopConf = new Configuration();
    Map<String, String> bypassConfigs =
        config.entrySet().stream()
            .filter(e -> e.getKey().startsWith(CATALOG_BYPASS_PREFIX))
            .collect(
                Collectors.toMap(
                    e -> e.getKey().substring(CATALOG_BYPASS_PREFIX.length()),
                    Map.Entry::getValue));
    bypassConfigs.forEach(hadoopConf::set);

    String catalogLocation =
        (String)
            CATALOG_PROPERTIES_METADATA.getOrDefault(
                config, HadoopCatalogPropertiesMetadata.LOCATION);
    this.catalogStorageLocation = Optional.ofNullable(catalogLocation).map(Path::new);
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    throw new UnsupportedOperationException();
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      List<SchemaEntity> schemas =
          store.list(namespace, SchemaEntity.class, Entity.EntityType.SCHEMA);
      return schemas.stream()
          .map(s -> NameIdentifier.of(namespace, s.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list schemas under namespace " + namespace, e);
    }
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      if (store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new SchemaAlreadyExistsException("Schema " + ident.name() + " already exists");
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident.name() + " exists", ioe);
    }

    String schemaLocation =
        (String)
            SCHEMA_PROPERTIES_METADATA.getOrDefault(
                properties, HadoopSchemaPropertiesMetadata.LOCATION);
    Path schemaPath =
        Optional.ofNullable(schemaLocation)
            .map(Path::new)
            .orElse(catalogStorageLocation.map(p -> new Path(p, ident.name())).orElse(null));

    if (schemaPath != null) {
      try {
        FileSystem fs = schemaPath.getFileSystem(hadoopConf);
        if (fs.exists(schemaPath)) {
          throw new SchemaAlreadyExistsException(
              "Schema " + ident.name() + " location " + schemaPath + " already exists");
        }

        if (!fs.mkdirs(schemaPath)) {
          // Fail the operation when failed to create the schema path.
          throw new RuntimeException(
              "Failed to create schema " + ident.name() + " location " + schemaPath);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(
            "Failed to create schema " + ident.name() + " location " + schemaPath, ioe);
      }

      LOG.info("Created schema {} location {}", ident.name(), schemaPath);
    }

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkNotNull(stringId, "Property String identifier should not be null");

    SchemaEntity schemaEntity =
        new SchemaEntity.Builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withProperties(addManagedFlagToProperties(properties))
            .withAuditInfo(
                new AuditInfo.Builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(schemaEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create schema " + ident.name(), ioe);
    }

    return new HadoopSchema.Builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(schemaEntity.properties())
        .withAuditInfo(schemaEntity.auditInfo())
        .build();
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      SchemaEntity schemaEntity = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);

      return new HadoopSchema.Builder()
          .withName(ident.name())
          .withComment(schemaEntity.comment())
          .withProperties(schemaEntity.properties())
          .withAuditInfo(schemaEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException("Schema " + ident.name() + " does not exist", exception);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + ident.name(), ioe);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      if (!store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException("Schema " + ident.name() + " does not exist");
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident.name() + " exists", ioe);
    }

    try {
      SchemaEntity entity =
          store.update(
              ident,
              SchemaEntity.class,
              Entity.EntityType.SCHEMA,
              schemaEntity -> updateSchemaEntity(ident, schemaEntity, changes));

      return new HadoopSchema.Builder()
          .withName(ident.name())
          .withComment(entity.comment())
          .withProperties(entity.properties())
          .withAuditInfo(entity.auditInfo())
          .build();

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to update schema " + ident.name(), ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchSchemaException("Schema " + ident.name() + " does not exist", nsee);
    } catch (AlreadyExistsException aee) {
      throw new RuntimeException(
          "Schema "
              + ident.name()
              + " already exists, this is unexpected because schema doesn't support rename",
          aee);
    }
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      SchemaEntity schemaEntity = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());

      String schemaLocation =
          (String)
              SCHEMA_PROPERTIES_METADATA.getOrDefault(
                  properties, HadoopSchemaPropertiesMetadata.LOCATION);
      Path schemaPath =
          Optional.ofNullable(schemaLocation)
              .map(Path::new)
              .orElse(catalogStorageLocation.map(p -> new Path(p, ident.name())).orElse(null));

      // Nothing to delete if the schema path is not set.
      if (schemaPath == null) {
        return true;
      }

      FileSystem fs = schemaPath.getFileSystem(hadoopConf);
      // Nothing to delete if the schema path does not exist.
      if (!fs.exists(schemaPath)) {
        return true;
      }

      if (fs.listStatus(schemaPath).length > 0 && !cascade) {
        throw new NonEmptySchemaException(
            "Schema " + ident.name() + " with location " + schemaPath + " is not empty");
      } else {
        fs.delete(schemaPath, true);
      }

      LOG.info("Deleted schema {} location {}", ident.name(), schemaPath);
      return true;

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident.name() + " location", ioe);
    }
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Hadoop fileset catalog doesn't support table related operations");
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    return FILESET_PROPERTIES_METADATA;
  }

  @Override
  public void close() throws IOException {}

  private Map<String, String> addManagedFlagToProperties(Map<String, String> properties) {
    return ImmutableMap.<String, String>builder()
        .putAll(properties)
        .put(HadoopSchemaPropertiesMetadata.GRAVITINO_MANAGED_ENTITY, Boolean.TRUE.toString())
        .build();
  }

  private SchemaEntity updateSchemaEntity(
      NameIdentifier ident, SchemaEntity schemaEntity, SchemaChange... changes) {
    Map<String, String> props = Maps.newHashMap(schemaEntity.properties());

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

    return new SchemaEntity.Builder()
        .withName(schemaEntity.name())
        .withNamespace(ident.namespace())
        .withId(schemaEntity.id())
        .withComment(schemaEntity.comment())
        .withProperties(props)
        .withAuditInfo(
            new AuditInfo.Builder()
                .withCreator(schemaEntity.auditInfo().creator())
                .withCreateTime(schemaEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }
}
