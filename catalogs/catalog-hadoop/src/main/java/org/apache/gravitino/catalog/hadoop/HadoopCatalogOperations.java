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
package org.apache.gravitino.catalog.hadoop;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.gravitino.file.Fileset.LOCATION_PLACEHOLDER_PREFIX;
import static org.apache.gravitino.file.Fileset.RESERVED_CATALOG_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.RESERVED_FILESET_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.RESERVED_SCHEMA_PLACEHOLDER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCatalogOperations extends ManagedSchemaOperations
    implements CatalogOperations, FilesetCatalog {
  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema %s does not exist";
  private static final String FILESET_DOES_NOT_EXIST_MSG = "Fileset %s does not exist";
  private static final String SLASH = "/";

  // location placeholder pattern format: {{placeholder}}
  private static final Pattern LOCATION_PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{(.*?)\\}\\}");
  private static final Logger LOG = LoggerFactory.getLogger(HadoopCatalogOperations.class);

  private final EntityStore store;

  private HasPropertyMetadata propertiesMetadata;

  @VisibleForTesting Configuration hadoopConf;

  @VisibleForTesting Optional<Path> catalogStorageLocation;

  private Map<String, String> conf;

  private CatalogInfo catalogInfo;

  private Map<String, FileSystemProvider> fileSystemProvidersMap;

  private FileSystemProvider defaultFileSystemProvider;

  HadoopCatalogOperations(EntityStore store) {
    this.store = store;
  }

  public HadoopCatalogOperations() {
    this(GravitinoEnv.getInstance().entityStore());
  }

  @Override
  public EntityStore store() {
    return store;
  }

  public CatalogInfo getCatalogInfo() {
    return catalogInfo;
  }

  public Configuration getHadoopConf() {
    Configuration configuration = new Configuration();
    conf.forEach((k, v) -> configuration.set(k.replace(CATALOG_BYPASS_PREFIX, ""), v));
    return configuration;
  }

  public Map<String, String> getConf() {
    return conf;
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.propertiesMetadata = propertiesMetadata;
    this.catalogInfo = info;

    this.conf = config;

    String fileSystemProviders =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(config, HadoopCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS);
    this.fileSystemProvidersMap =
        ImmutableMap.<String, FileSystemProvider>builder()
            .putAll(FileSystemUtils.getFileSystemProviders(fileSystemProviders))
            .build();

    String defaultFileSystemProviderName =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(config, HadoopCatalogPropertiesMetadata.DEFAULT_FS_PROVIDER);
    this.defaultFileSystemProvider =
        FileSystemUtils.getFileSystemProviderByName(
            fileSystemProvidersMap, defaultFileSystemProviderName);

    String catalogLocation =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(config, HadoopCatalogPropertiesMetadata.LOCATION);
    checkPlaceholderValue(catalogLocation);

    this.catalogStorageLocation =
        StringUtils.isNotBlank(catalogLocation)
            ? Optional.of(catalogLocation)
                .map(s -> s.endsWith(SLASH) ? s : s + SLASH)
                .map(Path::new)
            : Optional.empty();
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

      return HadoopFileset.builder()
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
    checkPlaceholderValue(storageLocation);

    Path filesetPath =
        caculateFilesetPath(
            schemaIdent.name(), ident.name(), storageLocation, schemaPath, properties);

    try {
      // formalize the path to avoid path without scheme, uri, authority, etc.
      FileSystem fs = getFileSystem(filesetPath, conf);
      filesetPath = filesetPath.makeQualified(fs.getUri(), fs.getWorkingDirectory());
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
        FilesetEntity.builder()
            .withName(ident.name())
            .withId(stringId.id())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withFilesetType(type)
            // Store the storageLocation to the store. If the "storageLocation" is null for managed
            // fileset, Gravitino will get and store the location based on the catalog/schema's
            // location and store it to the store.
            .withStorageLocation(filesetPath.toString())
            .withProperties(properties)
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

    return HadoopFileset.builder()
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

      return HadoopFileset.builder()
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
        FileSystem fs = getFileSystem(filesetPath, conf);
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
  public String getFileLocation(NameIdentifier ident, String subPath)
      throws NoSuchFilesetException {
    Preconditions.checkArgument(subPath != null, "subPath must not be null");
    String processedSubPath;
    if (!subPath.trim().isEmpty() && !subPath.trim().startsWith(SLASH)) {
      processedSubPath = SLASH + subPath.trim();
    } else {
      processedSubPath = subPath.trim();
    }

    Fileset fileset = loadFileset(ident);

    boolean isSingleFile = checkSingleFile(fileset);
    // if the storage location is a single file, it cannot have sub path to access.
    if (isSingleFile && StringUtils.isBlank(processedSubPath)) {
      throw new GravitinoRuntimeException(
          "Sub path should always be blank, because the fileset only mounts a single file.");
    }

    // do checks for some data operations.
    if (hasCallerContext()) {
      Map<String, String> contextMap = CallerContext.CallerContextHolder.get().context();
      String operation =
          contextMap.getOrDefault(
              FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
              FilesetDataOperation.UNKNOWN.name());
      if (!FilesetDataOperation.checkValid(operation)) {
        LOG.warn(
            "The data operation: {} is not valid, we cannot do some checks for this operation.",
            operation);
      } else {
        FilesetDataOperation dataOperation = FilesetDataOperation.valueOf(operation);
        switch (dataOperation) {
          case RENAME:
            // Fileset only mounts a single file, the storage location of the fileset cannot be
            // renamed; Otherwise the metadata in the Gravitino server may be inconsistent.
            if (isSingleFile) {
              throw new GravitinoRuntimeException(
                  "Cannot rename the fileset: %s which only mounts to a single file.", ident);
            }
            // if the sub path is blank, it cannot be renamed,
            // otherwise the metadata in the Gravitino server may be inconsistent.
            if (StringUtils.isBlank(processedSubPath)
                || (processedSubPath.startsWith(SLASH) && processedSubPath.length() == 1)) {
              throw new GravitinoRuntimeException(
                  "subPath cannot be blank when need to rename a file or a directory.");
            }
            break;
          default:
            break;
        }
      }
    }

    String fileLocation;
    // 1. if the storage location is a single file, we pass the storage location directly
    // 2. if the processed sub path is blank, we pass the storage location directly
    if (isSingleFile || StringUtils.isBlank(processedSubPath)) {
      fileLocation = fileset.storageLocation();
    } else {
      // the processed sub path always starts with "/" if it is not blank,
      // so we can safely remove the tailing slash if storage location ends with "/".
      String storageLocation =
          fileset.storageLocation().endsWith(SLASH)
              ? fileset.storageLocation().substring(0, fileset.storageLocation().length() - 1)
              : fileset.storageLocation();
      fileLocation = String.format("%s%s", storageLocation, processedSubPath);
    }
    return fileLocation;
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
    if (schemaPath != null && !containsPlaceholder(schemaPath.toString())) {
      try {
        FileSystem fs = getFileSystem(schemaPath, conf);
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

    return super.createSchema(ident, comment, properties);
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

    return super.alterSchema(ident, changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      Namespace filesetNs =
          NamespaceUtil.ofFileset(
              ident.namespace().level(0), // metalake name
              ident.namespace().level(1), // catalog name
              ident.name() // schema name
              );

      List<FilesetEntity> filesets =
          store.list(filesetNs, FilesetEntity.class, Entity.EntityType.FILESET);
      if (!filesets.isEmpty() && !cascade) {
        throw new NonEmptySchemaException("Schema %s is not empty", ident);
      }

      SchemaEntity schemaEntity = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());
      Path schemaPath = getSchemaPath(ident.name(), properties);

      boolean dropped = super.dropSchema(ident, cascade);
      if (!dropped) {
        return false;
      }

      // Delete all the managed filesets no matter whether the storage location is under the
      // schema path or not.
      // The reason why we delete the managed fileset's storage location one by one is because we
      // may mis-delete the storage location of the external fileset if it happens to be under
      // the schema path.
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      filesets
          .parallelStream()
          .filter(f -> f.filesetType() == Fileset.Type.MANAGED)
          .forEach(
              f -> {
                ClassLoader oldCl = Thread.currentThread().getContextClassLoader();
                try {
                  // parallelStream uses forkjoin thread pool, which has a different classloader
                  // than the catalog thread. We need to set the context classloader to the
                  // catalog's classloader to avoid classloading issues.
                  Thread.currentThread().setContextClassLoader(cl);
                  Path filesetPath = new Path(f.storageLocation());
                  FileSystem fs = getFileSystem(filesetPath, conf);
                  if (fs.exists(filesetPath)) {
                    if (!fs.delete(filesetPath, true)) {
                      LOG.warn("Failed to delete fileset {} location {}", f.name(), filesetPath);
                    }
                  }
                } catch (IOException ioe) {
                  LOG.warn(
                      "Failed to delete fileset {} location {}",
                      f.name(),
                      f.storageLocation(),
                      ioe);
                } finally {
                  Thread.currentThread().setContextClassLoader(oldCl);
                }
              });

      // Delete the schema path if it exists and is empty.
      if (schemaPath != null) {
        FileSystem fs = getFileSystem(schemaPath, conf);
        if (fs.exists(schemaPath)) {
          FileStatus[] statuses = fs.listStatus(schemaPath);
          if (statuses.length == 0) {
            if (fs.delete(schemaPath, true)) {
              LOG.info("Deleted schema {} location {}", ident, schemaPath);
            } else {
              LOG.warn(
                  "Failed to delete schema {} because it has files/folders under location {}",
                  ident,
                  schemaPath);
            }
          }
        }
      }

      LOG.info("Deleted schema {}", ident);
      return true;

    } catch (NoSuchEntityException ne) {
      LOG.warn("Schema {} does not exist", ident);
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident + " location", ioe);
    }
  }

  /**
   * Since the Hadoop catalog was completely managed by Gravitino, we don't need to test the
   * connection
   *
   * @param catalogIdent the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    // Do nothing
  }

  @Override
  public void close() throws IOException {}

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
      } else if (change instanceof FilesetChange.RemoveComment) {
        newComment = null;
      } else {
        throw new IllegalArgumentException(
            "Unsupported fileset change: " + change.getClass().getSimpleName());
      }
    }

    return FilesetEntity.builder()
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
            propertiesMetadata
                .schemaPropertiesMetadata()
                .getOrDefault(properties, HadoopSchemaPropertiesMetadata.LOCATION);
    checkPlaceholderValue(schemaLocation);

    return Optional.ofNullable(schemaLocation)
        .map(s -> s.endsWith(SLASH) ? s : s + SLASH)
        .map(Path::new)
        .orElse(
            catalogStorageLocation
                .map(p -> containsPlaceholder(p.toString()) ? p : new Path(p, name))
                .orElse(null));
  }

  /**
   * Check whether the placeholder in the location is valid. Throw an exception if the location
   * contains a placeholder with an empty value.
   *
   * @param location the location to check.
   */
  private void checkPlaceholderValue(String location) {
    if (StringUtils.isBlank(location)) {
      return;
    }

    Matcher matcher = LOCATION_PLACEHOLDER_PATTERN.matcher(location);
    while (matcher.find()) {
      String placeholder = matcher.group(1);
      if (placeholder.isEmpty()) {
        throw new IllegalArgumentException(
            "Placeholder in location should not be empty, location: " + location);
      }
    }
  }

  /**
   * Check whether the location contains a placeholder. The placeholder is in the format of
   * {{name}}.
   *
   * @param location the location to check.
   * @return true if the location contains a placeholder, false otherwise.
   */
  private boolean containsPlaceholder(String location) {
    return StringUtils.isNotBlank(location)
        && LOCATION_PLACEHOLDER_PATTERN.matcher(location).find();
  }

  private Path caculateFilesetPath(
      String schemaName,
      String filesetName,
      String storageLocation,
      Path schemaPath,
      Map<String, String> properties) {
    // The specified storageLocation will take precedence
    // case 1: storageLocation is not empty and does not contain placeholder
    if (StringUtils.isNotBlank(storageLocation) && !containsPlaceholder(storageLocation)) {
      return new Path(storageLocation);
    }

    Map<String, String> placeholderMapping = new HashMap<>();
    properties.forEach(
        (k, v) -> {
          if (k.startsWith(LOCATION_PLACEHOLDER_PREFIX)) {
            placeholderMapping.put(k.substring(LOCATION_PLACEHOLDER_PREFIX.length()), v);
          }
        });
    placeholderMapping.put(
        RESERVED_CATALOG_PLACEHOLDER.substring(LOCATION_PLACEHOLDER_PREFIX.length()),
        catalogInfo.name());
    placeholderMapping.put(
        RESERVED_SCHEMA_PLACEHOLDER.substring(LOCATION_PLACEHOLDER_PREFIX.length()), schemaName);
    placeholderMapping.put(
        RESERVED_FILESET_PLACEHOLDER.substring(LOCATION_PLACEHOLDER_PREFIX.length()), filesetName);

    // case 2: storageLocation is not empty and contains placeholder
    if (StringUtils.isNotBlank(storageLocation)) {
      return new Path(replacePlaceholders(storageLocation, placeholderMapping));
    }

    // case 3: storageLocation is empty and schemaPath does not contain placeholder
    if (!containsPlaceholder(schemaPath.toString())) {
      return new Path(schemaPath, filesetName);
    }

    // case 4: storageLocation is empty and schemaPath contains placeholder
    return new Path(replacePlaceholders(schemaPath.toString(), placeholderMapping));
  }

  private String replacePlaceholders(String location, Map<String, String> placeholderMapping) {
    Matcher matcher = LOCATION_PLACEHOLDER_PATTERN.matcher(location);
    StringBuilder result = new StringBuilder();
    int currentPosition = 0;
    while (matcher.find()) {
      // Append the text before the match
      result.append(location, currentPosition, matcher.start());

      // Append the replacement
      String key = matcher.group(1);
      String replacement = placeholderMapping.get(key);
      if (replacement == null) {
        throw new IllegalArgumentException("No value found for placeholder: " + key);
      }
      result.append(replacement);

      currentPosition = matcher.end();
    }

    // Append the rest of the text
    if (currentPosition < location.length()) {
      result.append(location, currentPosition, location.length());
    }
    return result.toString();
  }

  @VisibleForTesting
  Path formalizePath(Path path, Map<String, String> configuration) throws IOException {
    FileSystem defaultFs = getFileSystem(path, configuration);
    return path.makeQualified(defaultFs.getUri(), defaultFs.getWorkingDirectory());
  }

  private boolean hasCallerContext() {
    return CallerContext.CallerContextHolder.get() != null
        && CallerContext.CallerContextHolder.get().context() != null
        && !CallerContext.CallerContextHolder.get().context().isEmpty();
  }

  private boolean checkSingleFile(Fileset fileset) {
    try {
      Path locationPath = new Path(fileset.storageLocation());
      return getFileSystem(locationPath, conf).getFileStatus(locationPath).isFile();
    } catch (FileNotFoundException e) {
      // We should always return false here, same with the logic in `FileSystem.isFile(Path f)`.
      return false;
    } catch (IOException e) {
      throw new GravitinoRuntimeException(
          e,
          "Exception occurs when checking whether fileset: %s mounts a single file",
          fileset.name());
    }
  }

  FileSystem getFileSystem(Path path, Map<String, String> config) throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("Path should not be null");
    }

    String scheme =
        path.toUri().getScheme() != null
            ? path.toUri().getScheme()
            : defaultFileSystemProvider.scheme();

    FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
    if (provider == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported scheme: %s, path: %s, all supported schemes: %s and providers: %s",
              scheme, path, fileSystemProvidersMap.keySet(), fileSystemProvidersMap.values()));
    }

    int timeoutSeconds =
        (int)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(
                    config, HadoopCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS);
    try {
      AtomicReference<FileSystem> fileSystem = new AtomicReference<>();
      Awaitility.await()
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                fileSystem.set(provider.getFileSystem(path, config));
                return true;
              });
      return fileSystem.get();
    } catch (ConditionTimeoutException e) {
      throw new IOException(
          String.format(
              "Failed to get FileSystem for path: %s, scheme: %s, provider: %s, config: %s within %s "
                  + "seconds, please check the configuration or increase the "
                  + "file system connection timeout time by setting catalog property: %s",
              path,
              scheme,
              provider,
              config,
              timeoutSeconds,
              HadoopCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS),
          e);
    }
  }
}
