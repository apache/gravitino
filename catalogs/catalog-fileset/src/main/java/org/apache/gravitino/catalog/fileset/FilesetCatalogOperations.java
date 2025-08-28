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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_CATALOG_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.file.Fileset.PROPERTY_FILESET_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.PROPERTY_LOCATION_PLACEHOLDER_PREFIX;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;
import static org.apache.gravitino.file.Fileset.PROPERTY_SCHEMA_PLACEHOLDER;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
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
import org.apache.gravitino.catalog.FilesetFileOps;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.dto.file.FileInfoDTO;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.file.FileInfo;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilesetCatalogOperations extends ManagedSchemaOperations
    implements CatalogOperations, FilesetCatalog, FilesetFileOps {
  private static final String SCHEMA_DOES_NOT_EXIST_MSG = "Schema %s does not exist";
  private static final String FILESET_DOES_NOT_EXIST_MSG = "Fileset %s does not exist";
  private static final String SLASH = "/";

  // location placeholder pattern format: {{placeholder}}
  private static final Pattern LOCATION_PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{(.*?)\\}\\}");
  private static final Logger LOG = LoggerFactory.getLogger(FilesetCatalogOperations.class);

  private final EntityStore store;

  private HasPropertyMetadata propertiesMetadata;

  @VisibleForTesting Configuration hadoopConf;

  @VisibleForTesting Map<String, Path> catalogStorageLocations;

  @VisibleForTesting Map<String, String> conf;

  private CatalogInfo catalogInfo;

  private Map<String, FileSystemProvider> fileSystemProvidersMap;

  private FileSystemProvider defaultFileSystemProvider;

  private boolean disableFSOps;

  @VisibleForTesting ScheduledThreadPoolExecutor scheduler;
  @VisibleForTesting Cache<FileSystemCacheKey, FileSystem> fileSystemCache;

  FilesetCatalogOperations(EntityStore store) {
    this.store = store;
    scheduler =
        new ScheduledThreadPoolExecutor(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("file-system-cache-for-fileset" + "-%d")
                .build());

    this.fileSystemCache =
        Caffeine.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .removalListener(
                (ignored, value, cause) -> {
                  try {
                    ((FileSystem) value).close();
                  } catch (IOException e) {
                    LOG.warn("Failed to close FileSystem instance in cache", e);
                  }
                })
            .scheduler(Scheduler.forScheduledExecutorService(scheduler))
            .build();
  }

  static class FileSystemCacheKey {
    // When the path is a path without scheme such as 'file','hdfs', etc., then the scheme and
    // authority are both null
    @Nullable private final String scheme;
    @Nullable private final String authority;
    private final Map<String, String> conf;
    private final String currentUser;

    FileSystemCacheKey(String scheme, String authority, Map<String, String> conf) {
      this.scheme = scheme;
      this.authority = authority;
      this.conf = conf;

      try {
        this.currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        throw new RuntimeException("Failed to get current user", e);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FileSystemCacheKey)) {
        return false;
      }
      FileSystemCacheKey that = (FileSystemCacheKey) o;
      return conf.equals(that.conf)
          && (scheme == null ? that.scheme == null : scheme.equals(that.scheme))
          && (authority == null ? that.authority == null : authority.equals(that.authority))
          && currentUser.equals(that.currentUser);
    }

    @Override
    public int hashCode() {
      int result = conf.hashCode();
      result = 31 * result + (scheme == null ? 0 : scheme.hashCode());
      result = 31 * result + (authority == null ? 0 : authority.hashCode());
      result = 31 * result + currentUser.hashCode();
      return result;
    }
  }

  public FilesetCatalogOperations() {
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

    this.disableFSOps =
        (boolean)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(config, FilesetCatalogPropertiesMetadata.DISABLE_FILESYSTEM_OPS);
    if (!disableFSOps) {
      String fileSystemProviders =
          (String)
              propertiesMetadata
                  .catalogPropertiesMetadata()
                  .getOrDefault(config, FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS);
      this.fileSystemProvidersMap =
          ImmutableMap.<String, FileSystemProvider>builder()
              .putAll(FileSystemUtils.getFileSystemProviders(fileSystemProviders))
              .build();

      String defaultFileSystemProviderName =
          (String)
              propertiesMetadata
                  .catalogPropertiesMetadata()
                  .getOrDefault(config, FilesetCatalogPropertiesMetadata.DEFAULT_FS_PROVIDER);
      this.defaultFileSystemProvider =
          FileSystemUtils.getFileSystemProviderByName(
              fileSystemProvidersMap, defaultFileSystemProviderName);
    }

    this.catalogStorageLocations = getAndCheckCatalogStorageLocations(config);
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

      return FilesetImpl.builder()
          .withName(ident.name())
          .withType(filesetEntity.filesetType())
          .withComment(filesetEntity.comment())
          .withStorageLocations(filesetEntity.storageLocations())
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
  public FileInfo[] listFiles(NameIdentifier filesetIdent, String locationName, String subPath)
      throws NoSuchFilesetException, IOException {
    if (disableFSOps) {
      LOG.warn("Filesystem operations disabled, rejecting listFiles for {}", filesetIdent);
      throw new UnsupportedOperationException("Filesystem operations are disabled on this server");
    }

    String actualPath = getFileLocation(filesetIdent, subPath, locationName);

    FileSystem fileSystem = getFileSystemWithCache(new Path(actualPath), conf);
    Path formalizedPath =
        new Path(actualPath).makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());

    FileSystem fs = getFileSystemWithCache(formalizedPath, conf);
    if (!fs.exists(formalizedPath)) {
      throw new IllegalArgumentException(
          String.format(
              "Path %s does not exist in fileset %s", formalizedPath.toString(), filesetIdent));
    }

    String catalogName = filesetIdent.namespace().level(1);
    String schemaName = filesetIdent.namespace().level(2);
    String filesetName = filesetIdent.name();

    try {
      return Arrays.stream(fs.listStatus(formalizedPath))
          .map(
              status ->
                  FileInfoDTO.builder()
                      .name(status.getPath().getName())
                      .isDir(status.isDirectory())
                      .size(status.isDirectory() ? 0L : status.getLen())
                      .lastModified(status.getModificationTime())
                      .path(buildGVFSFilePath(catalogName, schemaName, filesetName, subPath))
                      .build())
          .toArray(FileInfo[]::new);

    } catch (IOException e) {
      throw new RuntimeException("Failed to list files in fileset" + filesetIdent, e);
    }
  }

  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    storageLocations.forEach(
        (name, path) -> {
          if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException("Location name must not be blank");
          }
          if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException(
                "Storage location must not be blank for location name: " + name);
          }
        });

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
    if (type == Fileset.Type.EXTERNAL) {
      if (storageLocations.isEmpty()) {
        throw new IllegalArgumentException(
            "Storage location must be set for external fileset " + ident);
      }
      storageLocations.forEach(
          (locationName, location) -> {
            if (StringUtils.isBlank(location)) {
              throw new IllegalArgumentException(
                  "Storage location must be set for external fileset "
                      + ident
                      + " with location name "
                      + locationName);
            }
          });
    }

    // Either catalog property "location", or schema property "location", or storageLocation must be
    // set for managed fileset.
    Map<String, Path> schemaPaths =
        getAndCheckSchemaPaths(schemaIdent.name(), schemaEntity.properties());
    if (schemaPaths.isEmpty() && storageLocations.isEmpty()) {
      throw new IllegalArgumentException(
          "Storage location must be set for fileset "
              + ident
              + " when it's catalog and schema location are not set");
    }
    storageLocations.forEach((k, location) -> checkPlaceholderValue(location));

    Map<String, Path> filesetPaths =
        calculateFilesetPaths(
            schemaIdent.name(), ident.name(), storageLocations, schemaPaths, properties);
    properties = setDefaultLocationIfAbsent(properties, filesetPaths);

    ImmutableMap.Builder<String, Path> filesetPathsBuilder = ImmutableMap.builder();
    if (disableFSOps) {
      filesetPaths.forEach(
          (locationName, location) -> {
            // If the location does not have scheme and filesystem operations are disabled in the
            // server side, we cannot formalize the path by filesystem, neither can we do in the
            // client side, so we should throw an exception here.
            if (location.toUri().getScheme() == null) {
              throw new IllegalArgumentException(
                  "Storage location must have scheme for fileset if filesystem operations are "
                      + "disabled in the server side, location: "
                      + location
                      + ", location name: "
                      + locationName);
            }

            filesetPathsBuilder.put(locationName, location);
          });
    } else {
      try {
        // formalize the path to avoid path without scheme, uri, authority, etc.
        for (Map.Entry<String, Path> entry : filesetPaths.entrySet()) {

          FileSystem tmpFs = getFileSystemWithCache(entry.getValue(), conf);
          Path formalizePath =
              entry.getValue().makeQualified(tmpFs.getUri(), tmpFs.getWorkingDirectory());

          filesetPathsBuilder.put(entry.getKey(), formalizePath);
          FileSystem fs = getFileSystemWithCache(formalizePath, conf);

          if (fs.exists(formalizePath) && fs.getFileStatus(formalizePath).isFile()) {
            throw new IllegalArgumentException(
                "Fileset location cannot be a file: "
                    + formalizePath
                    + ", location name: "
                    + entry.getKey());
          }

          if (!fs.exists(formalizePath)) {
            if (!fs.mkdirs(formalizePath)) {
              throw new RuntimeException(
                  "Failed to create fileset "
                      + ident
                      + " location "
                      + formalizePath
                      + " with location name "
                      + entry.getKey());
            }

            LOG.info(
                "Created fileset {} location {} with location name {}",
                ident,
                formalizePath,
                entry.getKey());
          } else {
            LOG.info(
                "Fileset {} manages the existing location {} with location name {}",
                ident,
                formalizePath,
                entry.getKey());
          }
        }

      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create fileset " + ident, ioe);
      }
    }

    Map<String, String> formattedStorageLocations =
        Maps.transformValues(filesetPathsBuilder.build(), Path::toString);
    validateLocationHierarchy(
        Maps.transformValues(schemaPaths, Path::toString), formattedStorageLocations);

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
            .withStorageLocations(formattedStorageLocations)
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

    return FilesetImpl.builder()
        .withName(ident.name())
        .withComment(comment)
        .withType(type)
        .withStorageLocations(formattedStorageLocations)
        .withProperties(filesetEntity.properties())
        .withAuditInfo(filesetEntity.auditInfo())
        .build();
  }

  private Map<String, String> setDefaultLocationIfAbsent(
      Map<String, String> properties, Map<String, Path> filesetPaths) {
    Preconditions.checkArgument(
        filesetPaths != null && !filesetPaths.isEmpty(), "Fileset paths must not be null or empty");

    if (filesetPaths.size() == 1) {
      // If the fileset has only one location, it is the default location.
      String defaultLocationName = filesetPaths.keySet().iterator().next();
      if (properties == null || properties.isEmpty()) {
        return Collections.singletonMap(PROPERTY_DEFAULT_LOCATION_NAME, defaultLocationName);
      }
      if (!properties.containsKey(PROPERTY_DEFAULT_LOCATION_NAME)) {
        return ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(PROPERTY_DEFAULT_LOCATION_NAME, defaultLocationName)
            .build();
      }

      Preconditions.checkArgument(
          defaultLocationName.equals(properties.get(PROPERTY_DEFAULT_LOCATION_NAME)),
          "Default location name must be the same as the fileset location name");
      return ImmutableMap.copyOf(properties);
    }

    // multiple locations
    Preconditions.checkArgument(
        properties != null
            && !properties.isEmpty()
            && properties.containsKey(PROPERTY_DEFAULT_LOCATION_NAME)
            && filesetPaths.containsKey(properties.get(PROPERTY_DEFAULT_LOCATION_NAME)),
        "The fileset property "
            + PROPERTY_DEFAULT_LOCATION_NAME
            + " must be set and must be one of the fileset locations, "
            + "location names: "
            + filesetPaths.keySet()
            + ", default location name: "
            + Optional.ofNullable(properties)
                .map(p -> p.get(PROPERTY_DEFAULT_LOCATION_NAME))
                .orElse(null));
    return ImmutableMap.copyOf(properties);
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

      return FilesetImpl.builder()
          .withName(updatedFilesetEntity.name())
          .withComment(updatedFilesetEntity.comment())
          .withType(updatedFilesetEntity.filesetType())
          .withStorageLocations(updatedFilesetEntity.storageLocations())
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

      // For managed fileset, we should delete the related files.
      if (!disableFSOps && filesetEntity.filesetType() == Fileset.Type.MANAGED) {
        AtomicReference<IOException> exception = new AtomicReference<>();
        Map<String, Path> storageLocations =
            Maps.transformValues(filesetEntity.storageLocations(), Path::new);
        storageLocations.forEach(
            (locationName, location) -> {
              try {
                FileSystem fs = getFileSystemWithCache(location, conf);
                if (fs.exists(location)) {
                  if (!fs.delete(location, true)) {
                    LOG.warn(
                        "Failed to delete fileset {} location {} with location name {}",
                        ident,
                        location,
                        locationName);
                  }
                } else {
                  LOG.warn(
                      "Fileset {} location {} with location name {} does not exist",
                      ident,
                      location,
                      locationName);
                }
              } catch (IOException ioe) {
                LOG.warn(
                    "Failed to delete fileset {} location {} with location name {}",
                    ident,
                    location,
                    locationName,
                    ioe);
                exception.set(ioe);
              }
            });
        if (exception.get() != null) {
          throw exception.get();
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
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException, NoSuchLocationNameException {
    Preconditions.checkArgument(subPath != null, "subPath must not be null");
    String processedSubPath;
    if (!subPath.trim().isEmpty() && !subPath.trim().startsWith(SLASH)) {
      processedSubPath = SLASH + subPath.trim();
    } else {
      processedSubPath = subPath.trim();
    }

    Fileset fileset = loadFileset(ident);
    String targetLocationName;
    if (locationName == null) {
      targetLocationName =
          fileset.storageLocations().size() == 1
              // to be compatible with the old version, the fileset in old version only has one
              // location and does not have the default-location-name property
              ? fileset.storageLocations().keySet().iterator().next()
              : fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME);
    } else {
      targetLocationName = locationName;
    }
    if (!fileset.storageLocations().containsKey(targetLocationName)) {
      throw new NoSuchLocationNameException(
          "Location name %s does not exist in fileset %s", targetLocationName, ident);
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
            // if the sub path is blank, it cannot be renamed otherwise the metadata in the
            // Gravitino server may be inconsistent.
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
    // If the processed sub path is blank, we pass the storage location directly
    if (StringUtils.isBlank(processedSubPath)) {
      fileLocation = fileset.storageLocations().get(targetLocationName);
    } else {
      // the processed sub path always starts with "/" if it is not blank,
      // so we can safely remove the tailing slash if the storage location ends with "/".
      String storageLocation =
          removeTrailingSlash(fileset.storageLocations().get(targetLocationName));
      fileLocation = String.format("%s%s", storageLocation, processedSubPath);
    }
    return fileLocation;
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    if (disableFSOps) {
      return super.createSchema(ident, comment, properties);
    }

    try {
      if (store.exists(ident, Entity.EntityType.SCHEMA)) {
        throw new SchemaAlreadyExistsException("Schema %s already exists", ident);
      }
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to check if schema " + ident + " exists", ioe);
    }

    Map<String, Path> schemaPaths = getAndCheckSchemaPaths(ident.name(), properties);
    schemaPaths.forEach(
        (locationName, schemaPath) -> {
          if (schemaPath != null && !containsPlaceholder(schemaPath.toString())) {
            try {
              FileSystem fs = getFileSystemWithCache(schemaPath, conf);
              if (fs.exists(schemaPath) && fs.getFileStatus(schemaPath).isFile()) {
                throw new IllegalArgumentException(
                    "Fileset schema location cannot be a file: "
                        + schemaPath
                        + ", location name: "
                        + locationName);
              }

              if (!fs.exists(schemaPath)) {
                if (!fs.mkdirs(schemaPath)) {
                  // Fail the operation when failed to create the schema path.
                  throw new RuntimeException(
                      "Failed to create schema "
                          + ident
                          + " location: "
                          + schemaPath
                          + " with location name: "
                          + locationName);
                }
                LOG.info(
                    "Created schema {} location: {} with location name: {}",
                    ident,
                    schemaPath,
                    locationName);
              } else {
                LOG.info(
                    "Schema {} manages the existing location: {} with location name: {}",
                    ident,
                    schemaPath,
                    locationName);
              }

            } catch (IOException ioe) {
              throw new RuntimeException(
                  "Failed to create schema " + ident + " location " + schemaPath, ioe);
            }
          }
        });

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

    // note: we need to invalidate the related fileset cache when the schema rename change is
    // supported.
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
      Map<String, Path> schemaPaths = getAndCheckSchemaPaths(ident.name(), properties);

      boolean dropped = super.dropSchema(ident, cascade);
      if (disableFSOps) {
        return dropped;
      }

      // If the schema entity is failed to be deleted, we should not delete the storage location
      // and return false immediately.
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
                  f.storageLocations()
                      .forEach(
                          (locationName, location) -> {
                            try {
                              Path filesetPath = new Path(location);
                              FileSystem fs = getFileSystemWithCache(filesetPath, conf);
                              if (fs.exists(filesetPath)) {
                                if (!fs.delete(filesetPath, true)) {
                                  LOG.warn(
                                      "Failed to delete fileset {} location: {} with location name: {}",
                                      f.name(),
                                      filesetPath,
                                      locationName);
                                }
                              }
                            } catch (IOException ioe) {
                              LOG.warn(
                                  "Failed to delete fileset {} location: {} with location name: {}",
                                  f.name(),
                                  location,
                                  locationName,
                                  ioe);
                            }
                          });
                } finally {
                  Thread.currentThread().setContextClassLoader(oldCl);
                }
              });

      // Delete the schema path if it exists and is empty.
      if (!schemaPaths.isEmpty()) {
        AtomicReference<RuntimeException> exception = new AtomicReference<>();
        schemaPaths.forEach(
            (locationName, schemaPath) -> {
              try {
                FileSystem fs = getFileSystemWithCache(schemaPath, conf);
                if (fs.exists(schemaPath)) {
                  FileStatus[] statuses = fs.listStatus(schemaPath);
                  if (statuses.length == 0) {
                    if (fs.delete(schemaPath, true)) {
                      LOG.info(
                          "Deleted schema {} location {} with location name {}",
                          ident,
                          schemaPath,
                          locationName);
                    } else {
                      LOG.warn(
                          "Failed to delete schema {} because it has files/folders under location {} with location name {}",
                          ident,
                          schemaPath,
                          locationName);
                    }
                  }
                }
              } catch (IOException ioe) {
                LOG.warn(
                    "Failed to delete schema {} location {} with location name {}",
                    ident,
                    schemaPath,
                    locationName,
                    ioe);
                exception.set(
                    new RuntimeException("Failed to delete schema " + ident + " location", ioe));
              }
            });
        if (exception.get() != null) {
          throw exception.get();
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
   * Since the Fileset catalog was completely managed by Gravitino, we don't need to test the
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
  public void close() throws IOException {
    // do nothing
    if (scheduler != null) {
      scheduler.shutdownNow();
    }

    if (fileSystemCache != null) {
      fileSystemCache
          .asMap()
          .forEach(
              (k, v) -> {
                try {
                  v.close();
                } catch (IOException e) {
                  LOG.warn("Failed to close FileSystem instance in cache", e);
                }
              });
      fileSystemCache.cleanUp();
    }
  }

  private void validateLocationHierarchy(
      Map<String, String> schemaLocations, Map<String, String> filesetLocations) {
    if (schemaLocations == null
        || filesetLocations == null
        || schemaLocations.isEmpty()
        || filesetLocations.isEmpty()) {
      return;
    }

    filesetLocations.forEach(
        (filesetLocationName, filesetLocation) ->
            schemaLocations.forEach(
                (schemaLocationName, schemaLocation) -> {
                  if (ensureTrailingSlash(schemaLocation)
                      .startsWith(ensureTrailingSlash(filesetLocation))) {
                    throw new IllegalArgumentException(
                        String.format(
                            "The fileset location %s with location name %s is not allowed "
                                + "to be the parent of the schema location %s with location name %s",
                            filesetLocation,
                            filesetLocationName,
                            schemaLocation,
                            schemaLocationName));
                  }
                }));
  }

  private boolean existBlankValue(Map<String, String> properties, String key) {
    return properties.containsKey(key) && StringUtils.isBlank(properties.get(key));
  }

  private Map<String, Path> getAndCheckCatalogStorageLocations(Map<String, String> properties) {
    if (existBlankValue(properties, FilesetCatalogPropertiesMetadata.LOCATION)) {
      // If the properties contain the catalog property "location", it must not be blank.
      throw new IllegalArgumentException(
          "The value of the catalog property "
              + FilesetCatalogPropertiesMetadata.LOCATION
              + " must not be blank");
    }

    ImmutableMap.Builder<String, Path> catalogStorageLocations = ImmutableMap.builder();
    String unnamedLocation =
        (String)
            propertiesMetadata
                .catalogPropertiesMetadata()
                .getOrDefault(properties, FilesetCatalogPropertiesMetadata.LOCATION);
    if (StringUtils.isNotBlank(unnamedLocation)) {
      checkPlaceholderValue(unnamedLocation);
      catalogStorageLocations.put(
          LOCATION_NAME_UNKNOWN, new Path(ensureTrailingSlash(unnamedLocation)));
    }

    properties.forEach(
        (k, v) -> {
          if (k.startsWith(PROPERTY_MULTIPLE_LOCATIONS_PREFIX)) {
            String locationName = k.substring(PROPERTY_MULTIPLE_LOCATIONS_PREFIX.length());
            if (StringUtils.isBlank(locationName)) {
              throw new IllegalArgumentException("Location name must not be blank");
            }

            if (StringUtils.isBlank(v)) {
              throw new IllegalArgumentException(
                  "Location value must not be blank for " + "location name: " + locationName);
            }

            checkPlaceholderValue(v);

            if (!disableFSOps && !containsPlaceholder(v)) {
              Path path = new Path(v);
              FileSystem fs = getFileSystemWithCache(path, conf);
              try {
                if (fs.exists(path) && fs.getFileStatus(path).isFile()) {
                  throw new IllegalArgumentException(
                      "Fileset catalog location cannot be a file: "
                          + v
                          + ", location name: "
                          + locationName);
                }
              } catch (IOException e) {
                throw new RuntimeException(
                    "Failed to check if fileset catalog location exists: " + v, e);
              }
            }

            catalogStorageLocations.put(locationName, new Path((ensureTrailingSlash(v))));
          }
        });
    return catalogStorageLocations.build();
  }

  private Map<String, Path> getAndCheckSchemaPaths(
      String schemaName, Map<String, String> schemaProps) {
    if (existBlankValue(schemaProps, FilesetSchemaPropertiesMetadata.LOCATION)) {
      // If the properties contain the schema property "location", it must not be blank.
      throw new IllegalArgumentException(
          "The value of the schema property "
              + FilesetSchemaPropertiesMetadata.LOCATION
              + " must not be blank");
    }

    Map<String, Path> schemaPaths = new HashMap<>();
    catalogStorageLocations.forEach(
        (name, path) -> {
          if (containsPlaceholder(path.toString())) {
            schemaPaths.put(name, path);
          } else {
            schemaPaths.put(name, new Path(path, schemaName));
          }
        });

    String unnamedSchemaLocation =
        (String)
            propertiesMetadata
                .schemaPropertiesMetadata()
                .getOrDefault(schemaProps, FilesetSchemaPropertiesMetadata.LOCATION);
    checkPlaceholderValue(unnamedSchemaLocation);
    Optional.ofNullable(unnamedSchemaLocation)
        .map(this::ensureTrailingSlash)
        .map(Path::new)
        .ifPresent(p -> schemaPaths.put(LOCATION_NAME_UNKNOWN, p));

    schemaProps.forEach(
        (k, path) -> {
          if (k.startsWith(PROPERTY_MULTIPLE_LOCATIONS_PREFIX)) {
            checkPlaceholderValue(path);
            String locationName = k.substring(PROPERTY_MULTIPLE_LOCATIONS_PREFIX.length());
            if (StringUtils.isBlank(locationName)) {
              throw new IllegalArgumentException("Location name must not be blank");
            }
            Optional.ofNullable(path)
                .map(this::ensureTrailingSlash)
                .map(Path::new)
                .ifPresent(p -> schemaPaths.put(locationName, p));
          }
        });
    return ImmutableMap.copyOf(schemaPaths);
  }

  private String ensureTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path : path + SLASH;
  }

  private String removeTrailingSlash(String path) {
    return path.endsWith(SLASH) ? path.substring(0, path.length() - 1) : path;
  }

  private String ensureLeadingSlash(String path) {
    return path.startsWith(SLASH) ? path : SLASH + path;
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
        .withStorageLocations(filesetEntity.storageLocations())
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

  private Map<String, Path> calculateFilesetPaths(
      String schemaName,
      String filesetName,
      Map<String, String> storageLocations,
      Map<String, Path> schemaPaths,
      Map<String, String> properties) {
    ImmutableMap.Builder<String, Path> filesetPaths = ImmutableMap.builder();
    Set<String> locationNames = new HashSet<>(schemaPaths.keySet());
    locationNames.addAll(storageLocations.keySet());

    locationNames.forEach(
        locationName -> {
          String storageLocation = storageLocations.get(locationName);
          Path schemaPath = schemaPaths.get(locationName);
          filesetPaths.put(
              locationName,
              calculateFilesetPath(
                  schemaName, filesetName, storageLocation, schemaPath, properties));
        });
    return filesetPaths.build();
  }

  private Path calculateFilesetPath(
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
          if (k.startsWith(PROPERTY_LOCATION_PLACEHOLDER_PREFIX)) {
            placeholderMapping.put(k.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()), v);
          }
        });
    placeholderMapping.put(
        PROPERTY_CATALOG_PLACEHOLDER.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()),
        catalogInfo.name());
    placeholderMapping.put(
        PROPERTY_SCHEMA_PLACEHOLDER.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()),
        schemaName);
    placeholderMapping.put(
        PROPERTY_FILESET_PLACEHOLDER.substring(PROPERTY_LOCATION_PLACEHOLDER_PREFIX.length()),
        filesetName);

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

  @VisibleForTesting
  FileSystem getFileSystemWithCache(Path path, Map<String, String> conf) {
    String scheme = path.toUri().getScheme();
    String authority = path.toUri().getAuthority();
    return fileSystemCache.get(
        new FileSystemCacheKey(scheme, authority, conf),
        cacheKey -> {
          try {
            return getFileSystem(path, conf);
          } catch (IOException e) {
            throw new GravitinoRuntimeException(
                e, "Failed to get FileSystem for fileset: path: %s, conf: %s", path, conf);
          }
        });
  }

  private String buildGVFSFilePath(
      String catalogName, String schemaName, String filesetName, String subPath) {
    String prefix = String.join(SLASH, "/fileset", catalogName, schemaName, filesetName);
    return StringUtils.isBlank(subPath)
        ? prefix
        : prefix + ensureLeadingSlash(removeTrailingSlash(subPath));
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
                    config, FilesetCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS);
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
              FilesetCatalogPropertiesMetadata.FILESYSTEM_CONNECTION_TIMEOUT_SECONDS),
          e);
    }
  }
}
