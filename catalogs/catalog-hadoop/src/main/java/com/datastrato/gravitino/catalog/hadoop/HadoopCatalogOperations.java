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
import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
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
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.SupportsSchemas;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCatalogOperations implements CatalogOperations, SupportsSchemas, FilesetCatalog {

  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema %s does not exist";
  private static final String FILESET_DOES_NOT_EXIST_MSG = "Fileset %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(HadoopCatalogOperations.class);

  private static final HadoopCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new HadoopCatalogPropertiesMetadata();

  private static final HadoopSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new HadoopSchemaPropertiesMetadata();

  private static final HadoopFilesetPropertiesMetadata FILESET_PROPERTIES_METADATA =
      new HadoopFilesetPropertiesMetadata();

  private final CatalogEntity entity;

  private final EntityStore store;

  @VisibleForTesting Configuration hadoopConf;

  @VisibleForTesting Optional<Path> catalogStorageLocation;

  // For testing only.
  HadoopCatalogOperations(CatalogEntity entity, EntityStore store) {
    this.entity = entity;
    this.store = store;
  }

  public HadoopCatalogOperations(CatalogEntity entity) {
    this(entity, GravitinoEnv.getInstance().entityStore());
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
    try {
      NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
      if (!store.exists(schemaIdent, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException(SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
      }

      List<FilesetEntity> filesets =
          store.list(namespace, FilesetEntity.class, Entity.EntityType.FILESET);
      return filesets.stream()
          .map(f -> NameIdentifier.of(namespace, f.name()))
          .toArray(NameIdentifier[]::new);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list filesets under namespace " + namespace, e);
    }
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    try {
      FilesetEntity filesetEntity =
          store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);

      return new HadoopFileset.Builder()
          .withName(ident.name())
          .withType(filesetEntity.filesetType())
          .withComment(filesetEntity.comment())
          .withStorageLocation(filesetEntity.storageLocation())
          .withProperties(filesetEntity.properties())
          .withAuditInfo(filesetEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchFilesetException(exception, FILESET_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load fileset %s" + ident, ioe);
    }
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    try {
      if (store.exists(ident, Entity.EntityType.FILESET)) {
        throw new FilesetAlreadyExistsException("Fileset %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if fileset " + ident + " exists", ioe);
    }

    SchemaEntity schemaEntity;
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      schemaEntity = store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class);
    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, schemaIdent);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + schemaIdent, ioe);
    }

    // For external fileset, the storageLocation must be set.
    if (type == Fileset.Type.EXTERNAL && StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException(
          "Storage location must be set for external fileset " + ident);
    }

    // Either catalog property "location", or schema property "location", or storageLocation must be
    // set for managed fileset.
    Path schemaPath = getSchemaPath(schemaIdent.name(), schemaEntity.properties());
    if (schemaPath == null && StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException(
          "Storage location must be set for fileset "
              + ident
              + " when it's catalog and schema location are not set");
    }

    // The specified storageLocation will take precedence over the calculated one.
    Path filesetPath =
        StringUtils.isNotBlank(storageLocation)
            ? new Path(storageLocation)
            : new Path(schemaPath, ident.name());

    try {
      // formalize the path to avoid path without scheme, uri, authority, etc.
      filesetPath = formalizePath(filesetPath, hadoopConf);
      FileSystem fs = filesetPath.getFileSystem(hadoopConf);
      if (!fs.exists(filesetPath)) {
        if (!fs.mkdirs(filesetPath)) {
          throw new RuntimeException(
              "Failed to create fileset " + ident + " location " + filesetPath);
        }

        LOG.info("Created fileset {} location {}", ident, filesetPath);
      } else {
        LOG.info("Fileset {} manages the existing location {}", ident, filesetPath);
      }

    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to create fileset " + ident + " location " + filesetPath, ioe);
    }

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkArgument(stringId != null, "Property String identifier should not be null");

    FilesetEntity filesetEntity =
        new FilesetEntity.Builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withFilesetType(type)
            // Store the storageLocation to the store. If the "storageLocation" is null for
            // managed fileset, Gravitino will get and store the location based on the
            // catalog/schema's location and store it to the store.
            .withStorageLocation(filesetPath.toString())
            .withProperties(addManagedFlagToProperties(properties))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(filesetEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident, ioe);
    }

    return new HadoopFileset.Builder()
        .withName(ident.name())
        .withComment(comment)
        .withType(type)
        .withStorageLocation(filesetPath.toString())
        .withProperties(filesetEntity.properties())
        .withAuditInfo(filesetEntity.auditInfo())
        .build();
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    try {
      if (!store.exists(ident, Entity.EntityType.FILESET)) {
        throw new NoSuchFilesetException(FILESET_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load fileset " + ident, ioe);
    }

    try {
      FilesetEntity updatedFilesetEntity =
          store.update(
              ident,
              FilesetEntity.class,
              Entity.EntityType.FILESET,
              e -> updateFilesetEntity(ident, e, changes));

      return new HadoopFileset.Builder()
          .withName(updatedFilesetEntity.name())
          .withComment(updatedFilesetEntity.comment())
          .withType(updatedFilesetEntity.filesetType())
          .withStorageLocation(updatedFilesetEntity.storageLocation())
          .withProperties(updatedFilesetEntity.properties())
          .withAuditInfo(updatedFilesetEntity.auditInfo())
          .build();

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to update fileset " + ident, ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchFilesetException(nsee, FILESET_DOES_NOT_EXIST_MSG, ident);
    } catch (AlreadyExistsException aee) {
      // This is happened when renaming a fileset to an existing fileset name.
      throw new RuntimeException(
          "Fileset with the same name " + ident.name() + " already exists", aee);
    }
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    try {
      FilesetEntity filesetEntity =
          store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);
      Path filesetPath = new Path(filesetEntity.storageLocation());

      // For managed fileset, we should delete the related files.
      if (filesetEntity.filesetType() == Fileset.Type.MANAGED) {
        FileSystem fs = filesetPath.getFileSystem(hadoopConf);
        if (fs.exists(filesetPath)) {
          if (!fs.delete(filesetPath, true)) {
            LOG.warn("Failed to delete fileset {} location {}", ident, filesetPath);
            return false;
          }

        } else {
          LOG.warn("Fileset {} location {} does not exist", ident, filesetPath);
        }
      }

      return store.delete(ident, Entity.EntityType.FILESET);
    } catch (NoSuchEntityException ne) {
      LOG.warn("Fileset {} does not exist", ident);
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete fileset " + ident, ioe);
    }
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
        throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident + " exists", ioe);
    }

    Path schemaPath = getSchemaPath(ident.name(), properties);
    if (schemaPath != null) {
      try {
        FileSystem fs = schemaPath.getFileSystem(hadoopConf);
        if (!fs.exists(schemaPath)) {
          if (!fs.mkdirs(schemaPath)) {
            // Fail the operation when failed to create the schema path.
            throw new RuntimeException(
                "Failed to create schema " + ident + " location " + schemaPath);
          }
          LOG.info("Created schema {} location {}", ident, schemaPath);
        } else {
          LOG.info("Schema {} manages the existing location {}", ident, schemaPath);
        }

      } catch (IOException ioe) {
        throw new RuntimeException(
            "Failed to create schema " + ident + " location " + schemaPath, ioe);
      }
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
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    try {
      store.put(schemaEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create schema " + ident, ioe);
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
      throw new NoSuchSchemaException(exception, SCHEMA_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + ident, ioe);
    }
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      if (!store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new NoSuchSchemaException(SCHEMA_DOES_NOT_EXIST_MSG, ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident + " exists", ioe);
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
      SchemaEntity schemaEntity = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());

      Path schemaPath = getSchemaPath(ident.name(), properties);
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
            "Schema %s with location %s is not empty", ident, schemaPath);
      } else {
        fs.delete(schemaPath, true);
      }

      LOG.info("Deleted schema {} location {}", ident, schemaPath);
      return true;

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident + " location", ioe);
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
        .put(BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY, Boolean.TRUE.toString())
        .build();
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

    return new SchemaEntity.Builder()
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

  private FilesetEntity updateFilesetEntity(
      NameIdentifier ident, FilesetEntity filesetEntity, FilesetChange... changes) {
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());
    String newName = ident.name();
    String newComment = filesetEntity.comment();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty removeProperty = (FilesetChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        newName = ((FilesetChange.RenameFileset) change).getNewName();
      } else if (change instanceof FilesetChange.UpdateFilesetComment) {
        newComment = ((FilesetChange.UpdateFilesetComment) change).getNewComment();
      } else {
        throw new IllegalArgumentException(
            "Unsupported fileset change: " + change.getClass().getSimpleName());
      }
    }

    return new FilesetEntity.Builder()
        .withName(newName)
        .withNamespace(ident.namespace())
        .withId(filesetEntity.id())
        .withComment(newComment)
        .withFilesetType(filesetEntity.filesetType())
        .withStorageLocation(filesetEntity.storageLocation())
        .withProperties(props)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(filesetEntity.auditInfo().creator())
                .withCreateTime(filesetEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  private Path getSchemaPath(String name, Map<String, String> properties) {
    String schemaLocation =
        (String)
            SCHEMA_PROPERTIES_METADATA.getOrDefault(
                properties, HadoopSchemaPropertiesMetadata.LOCATION);

    return Optional.ofNullable(schemaLocation)
        .map(Path::new)
        .orElse(catalogStorageLocation.map(p -> new Path(p, name)).orElse(null));
  }

  @VisibleForTesting
  static Path formalizePath(Path path, Configuration configuration) throws IOException {
    FileSystem defaultFs = FileSystem.get(configuration);
    return path.makeQualified(defaultFs.getUri(), defaultFs.getWorkingDirectory());
  }
}
