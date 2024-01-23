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
import com.datastrato.gravitino.meta.FilesetEntity;
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
import java.util.Arrays;
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
    try {
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
      Path filesetPath = getFilesetPath(ident, filesetEntity);

      return new HadoopFileset.Builder()
          .withName(ident.name())
          .withType(filesetEntity.filesetType())
          .withComment(filesetEntity.comment())
          // The returned storageLocation is a calculated one. If the fileset is managed and
          // storageLocation is null, then the calculated location is the schema location + fileset
          // name.
          .withStorageLocation(filesetPath.toString())
          .withProperties(filesetEntity.properties())
          .withAuditInfo(filesetEntity.auditInfo())
          .build();

    } catch (NoSuchEntityException exception) {
      throw new NoSuchFilesetException("Fileset " + ident.name() + " does not exist", exception);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load fileset " + ident.name(), ioe);
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
        throw new FilesetAlreadyExistsException("Fileset " + ident.name() + " already exists");
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if fileset " + ident.name() + " exists", ioe);
    }

    SchemaEntity schemaEntity;
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    try {
      schemaEntity = store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class);
    } catch (NoSuchEntityException exception) {
      throw new NoSuchSchemaException(
          "Schema " + schemaIdent.name() + " does not exist", exception);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load schema " + schemaIdent.name(), ioe);
    }

    // For external fileset, the storageLocation must be set.
    if (type.equals(Fileset.Type.EXTERNAL) && StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException(
          "Storage location must be set for external fileset " + ident.name());
    }

    // Either catalog property "location", or schema property "location", or storageLocation must be
    // set for managed fileset.
    Path schemaPath = getSchemaPath(schemaIdent.name(), schemaEntity.properties());
    if (schemaPath == null && StringUtils.isBlank(storageLocation)) {
      throw new IllegalArgumentException(
          "Storage location must be set for fileset "
              + ident.name()
              + " when it's catalog and schema location are not set");
    }

    // The specified storageLocation will take precedence over the calculated one.
    Path filesetPath =
        StringUtils.isNotBlank(storageLocation)
            ? new Path(storageLocation)
            : new Path(schemaPath, ident.name());
    try {
      FileSystem fs = filesetPath.getFileSystem(hadoopConf);
      if (!fs.exists(filesetPath)) {
        if (!fs.mkdirs(filesetPath)) {
          throw new RuntimeException(
              "Failed to create fileset " + ident.name() + " location " + filesetPath);
        }
      } else {
        LOG.info("Fileset {} manages the existing location {}", ident.name(), filesetPath);
      }

    } catch (IOException ioe) {
      throw new RuntimeException(
          "Failed to create fileset " + ident.name() + " location " + filesetPath, ioe);
    }

    StringIdentifier stringId = StringIdentifier.fromProperties(properties);
    Preconditions.checkNotNull(stringId, "Property String identifier should not be null");

    FilesetEntity filesetEntity =
        new FilesetEntity.Builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withFilesetType(type)
            // Store the storageLocation to the store. The "storageLocation" can be null for managed
            // fileset, Gravitino will get the location based on the catalog/schema's location.
            .withStorageLocation(storageLocation)
            .withProperties(addManagedFlagToProperties(properties))
            .withAuditInfo(
                new AuditInfo.Builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(filesetEntity, true /* overwrite */);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident.name(), ioe);
    }

    return new HadoopFileset.Builder()
        .withName(ident.name())
        .withComment(comment)
        .withType(type)
        // The returned storageLocation is a calculated one. If the fileset is managed and
        // storageLocation is null, then the calculated location is the
        // schema location + fileset name.
        .withStorageLocation(filesetPath.toString())
        .withProperties(filesetEntity.properties())
        .withAuditInfo(filesetEntity.auditInfo())
        .build();
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    FilesetEntity filesetEntity;
    try {
      filesetEntity = store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);
    } catch (NoSuchEntityException exception) {
      throw new NoSuchFilesetException("Fileset " + ident.name() + " does not exist", exception);
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to load fileset " + ident.name(), ioe);
    }

    List<FilesetChange> renames =
        Arrays.stream(changes)
            .filter(c -> c instanceof FilesetChange.RenameFileset)
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        renames.size() <= 1,
        "Only at most one rename operation is allowed once for fileset operations: "
            + ident.name());

    String newPath = null;
    if (!renames.isEmpty()) {
      FilesetChange.RenameFileset rename = (FilesetChange.RenameFileset) renames.get(0);
      String newName = rename.getNewName();
      newPath = renameStorageLocation(ident, filesetEntity, newName);
    } else {
      newPath = getFilesetPath(ident, filesetEntity).toString();
    }

    try {
      FilesetEntity entity =
          store.update(
              ident,
              FilesetEntity.class,
              Entity.EntityType.FILESET,
              e -> updateFilesetEntity(ident, e, changes));

      return new HadoopFileset.Builder()
          .withName(entity.name())
          .withComment(entity.comment())
          .withType(entity.filesetType())
          .withStorageLocation(newPath)
          .withProperties(entity.properties())
          .withAuditInfo(entity.auditInfo())
          .build();

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to update fileset " + ident.name(), ioe);
    } catch (NoSuchEntityException nsee) {
      throw new NoSuchFilesetException("Fileset " + ident.name() + " does not exist", nsee);
    } catch (AlreadyExistsException aee) {
      // This is happened when renaming a fileset to an existing fileset name.
      throw new RuntimeException("Fileset " + ident.name() + " already exists", aee);
    }
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    try {
      FilesetEntity filesetEntity =
          store.get(ident, Entity.EntityType.FILESET, FilesetEntity.class);

      Path filesetPath = getFilesetPath(ident, filesetEntity);

      // For managed fileset, we should delete the related files.
      if (filesetEntity.filesetType().equals(Fileset.Type.MANAGED)) {
        FileSystem fs = filesetPath.getFileSystem(hadoopConf);
        if (fs.exists(filesetPath)) {
          if (!fs.delete(filesetPath, true)) {
            LOG.warn("Failed to delete fileset {} location {}", ident.name(), filesetPath);
            return false;
          }

        } else {
          LOG.warn("Fileset {} location {} does not exist", ident.name(), filesetPath);
        }
      }

      return store.delete(ident, Entity.EntityType.FILESET);
    } catch (NoSuchEntityException ne) {
      LOG.warn("Fileset {} does not exist", ident.name());
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete fileset " + ident.name(), ioe);
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
        throw new SchemaAlreadyExistsException("Schema " + ident.name() + " already exists");
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident.name() + " exists", ioe);
    }

    Path schemaPath = getSchemaPath(ident.name(), properties);
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
            new AuditInfo.Builder()
                .withCreator(schemaEntity.auditInfo().creator())
                .withCreateTime(schemaEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  private String renameStorageLocation(
      NameIdentifier nameIdent, FilesetEntity filesetEntity, String newName) {
    if (StringUtils.isNotBlank(filesetEntity.storageLocation())) {
      // For specified storageLocation, we should not rename the physical location.
      return filesetEntity.storageLocation();

    } else if (filesetEntity.filesetType().equals(Fileset.Type.MANAGED)) {
      // For managed fileset with no storage location, we should also rename the physical location.
      Path oldFilesetPath = getFilesetPath(nameIdent, filesetEntity);
      Path newFilesetPath = new Path(oldFilesetPath.getParent(), newName);

      try {
        FileSystem fs = oldFilesetPath.getFileSystem(hadoopConf);
        if (!fs.exists(oldFilesetPath)) {
          throw new NoSuchFilesetException(
              "Fileset " + nameIdent.name() + " location " + oldFilesetPath + " does not exist");
        }
        if (fs.exists(newFilesetPath)) {
          throw new RuntimeException(
              "Renamed Fileset " + newName + " location " + newFilesetPath + " already exists");
        }

        if (!fs.rename(oldFilesetPath, newFilesetPath)) {
          throw new RuntimeException(
              "Failed to rename fileset location from "
                  + oldFilesetPath
                  + " to new location "
                  + newFilesetPath);
        }

        return newFilesetPath.toString();

      } catch (IOException ioe) {
        throw new RuntimeException(
            "Failed to rename fileset location from "
                + oldFilesetPath
                + " to new location "
                + newFilesetPath,
            ioe);
      }
    } else {
      throw new IllegalArgumentException(
          "Storage location should not be null for external fileset: " + nameIdent.name());
    }
  }

  private FilesetEntity updateFilesetEntity(
      NameIdentifier ident, FilesetEntity filesetEntity, FilesetChange... changes) {
    Map<String, String> props =
        filesetEntity.properties() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(filesetEntity.properties());
    String newName = ident.name();

    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) change;
        props.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty removeProperty = (FilesetChange.RemoveProperty) change;
        props.remove(removeProperty.getProperty());
      } else if (change instanceof FilesetChange.RenameFileset) {
        newName = ((FilesetChange.RenameFileset) change).getNewName();
      } else {
        throw new IllegalArgumentException(
            "Unsupported fileset change: " + change.getClass().getSimpleName());
      }
    }

    return new FilesetEntity.Builder()
        .withName(newName)
        .withNamespace(ident.namespace())
        .withId(filesetEntity.id())
        .withComment(filesetEntity.comment())
        .withFilesetType(filesetEntity.filesetType())
        .withStorageLocation(filesetEntity.storageLocation())
        .withProperties(props)
        .withAuditInfo(
            new AuditInfo.Builder()
                .withCreator(filesetEntity.auditInfo().creator())
                .withCreateTime(filesetEntity.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
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

  private Path getFilesetPath(NameIdentifier ident, FilesetEntity filesetEntity) {
    if (StringUtils.isNotBlank(filesetEntity.storageLocation())) {
      return new Path(filesetEntity.storageLocation());

    } else if (filesetEntity.filesetType().equals(Fileset.Type.MANAGED)) {
      // If the fileset is managed and storageLocation is null, then the calculated location is the
      // schema location + fileset name.
      SchemaEntity schemaEntity;
      NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
      try {
        schemaEntity = store.get(schemaIdent, Entity.EntityType.SCHEMA, SchemaEntity.class);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to load schema " + schemaIdent.name() + " when doing fileset rename", e);
      }

      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());
      Path schemaPath = getSchemaPath(schemaIdent.name(), properties);
      Preconditions.checkArgument(
          schemaPath != null,
          "Schema location should not be null for managed fileset: " + ident.name());

      return new Path(schemaPath, ident.name());

    } else {
      throw new IllegalArgumentException(
          "Storage location should not be null for external fileset: " + ident.name());
    }
  }
}
