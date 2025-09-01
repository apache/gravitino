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
package org.apache.gravitino.filesystem.hadoop;

import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CATALOG_CACHE_ENABLE;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CATALOG_CACHE_ENABLE_DEFAULT;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.extractIdentifier;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getConfigMap;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getSubPathFromGvfsPath;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider;
import org.apache.gravitino.catalog.hadoop.fs.GravitinoFileSystemCredentialsProvider;
import org.apache.gravitino.catalog.hadoop.fs.SupportsCredentialVending;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for Gravitino Virtual File System operations.
 *
 * <p>This class provides the core functionality for interacting with the Gravitino Virtual File
 * System, including operations for file manipulation, directory management, and credential
 * handling. It handles the mapping between virtual paths in the Gravitino namespace and actual file
 * paths in underlying storage systems.
 *
 * <p>Implementations of this class should provide the specific behaviors for each file system
 * operation.
 */
public abstract class BaseGVFSOperations implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BaseGVFSOperations.class);
  private static final String SLASH = "/";
  private static final Set<String> CATALOG_NECESSARY_PROPERTIES_TO_KEEP =
      Sets.newHashSet(
          OSSProperties.GRAVITINO_OSS_ENDPOINT,
          OSSProperties.GRAVITINO_OSS_REGION,
          S3Properties.GRAVITINO_S3_ENDPOINT,
          S3Properties.GRAVITINO_S3_REGION,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);

  private final String metalakeName;

  private final Optional<FilesetMetadataCache> filesetMetadataCache;
  private final GravitinoClient gravitinoClient;

  private final Configuration conf;

  // Fileset nameIdentifier-locationName Pair and its corresponding FileSystem cache, the name
  // identifier has four levels, the first level is metalake name.
  private final Cache<Pair<NameIdentifier, String>, FileSystem> internalFileSystemCache;

  private final Map<String, FileSystemProvider> fileSystemProvidersMap;

  private final String currentLocationEnvVar;

  @Nullable private final String currentLocationName;

  private final long defaultBlockSize;

  private final boolean enableCredentialVending;

  /**
   * Constructs a new {@link BaseGVFSOperations} with the given {@link Configuration}.
   *
   * @param configuration the configuration
   */
  protected BaseGVFSOperations(Configuration configuration) {
    this.metalakeName =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    this.gravitinoClient = GravitinoVirtualFileSystemUtils.createClient(configuration);
    boolean enableFilesetCatalogCache =
        configuration.getBoolean(
            FS_GRAVITINO_FILESET_CATALOG_CACHE_ENABLE,
            FS_GRAVITINO_FILESET_CATALOG_CACHE_ENABLE_DEFAULT);
    this.filesetMetadataCache =
        enableFilesetCatalogCache
            ? Optional.of(new FilesetMetadataCache(gravitinoClient))
            : Optional.empty();

    this.internalFileSystemCache = newFileSystemCache(configuration);

    this.fileSystemProvidersMap = ImmutableMap.copyOf(getFileSystemProviders());

    this.currentLocationEnvVar =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_CURRENT_LOCATION_NAME_ENV_VAR_DEFAULT);
    this.currentLocationName = initCurrentLocationName(configuration);

    this.defaultBlockSize =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_BLOCK_SIZE,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_BLOCK_SIZE_DEFAULT);

    this.enableCredentialVending =
        configuration.getBoolean(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_ENABLE_CREDENTIAL_VENDING,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_ENABLE_CREDENTIAL_VENDING_DEFAULT);
    this.conf = configuration;
  }

  @Override
  public void close() throws IOException {
    // close all actual FileSystems
    for (FileSystem fileSystem : internalFileSystemCache.asMap().values()) {
      try {
        fileSystem.close();
      } catch (IOException e) {
        // ignore
      }
    }
    internalFileSystemCache.invalidateAll();

    try {
      if (filesetMetadataCache.isPresent()) {
        filesetMetadataCache.get().close();
      }
    } catch (IOException e) {
      // ignore
    }
  }

  /**
   * Open a file for reading. Same as {@link FileSystem#open(Path, int)}.
   *
   * @param gvfsPath the virtual path of the file.
   * @param bufferSize the buffer size.
   * @return the input stream.
   * @throws IOException if an I/O error occurs.
   */
  public abstract FSDataInputStream open(Path gvfsPath, int bufferSize) throws IOException;

  /**
   * Set the working directory. Same as {@link FileSystem#setWorkingDirectory(Path)}.
   *
   * @param gvfsDir the new working directory.
   * @throws FilesetPathNotFoundException if the fileset path is not found.
   */
  public abstract void setWorkingDirectory(Path gvfsDir) throws FilesetPathNotFoundException;

  /**
   * Create a file. Same as {@link FileSystem#create(Path, FsPermission, boolean, int, short, long,
   * Progressable)}.
   *
   * @param gvfsPath the virtual path of the file.
   * @param permission the permission.
   * @param overwrite whether to overwrite the file if it exists.
   * @param bufferSize the buffer size.
   * @param replication the replication factor.
   * @param blockSize the block size.
   * @param progress the progressable.
   * @return the output stream.
   * @throws IOException if an I/O error occurs.
   */
  public abstract FSDataOutputStream create(
      Path gvfsPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException;

  /**
   * Append to an existing file. Same as {@link FileSystem#append(Path, int, Progressable)}.
   *
   * @param gvfsPath the virtual path of the file.
   * @param bufferSize the buffer size.
   * @param progress the progressable.
   * @return the output stream.
   * @throws IOException if an I/O error occurs.
   */
  public abstract FSDataOutputStream append(Path gvfsPath, int bufferSize, Progressable progress)
      throws IOException;

  /**
   * Rename a file. Same as {@link FileSystem#rename(Path, Path)}.
   *
   * @param srcGvfsPath the virtual source path.
   * @param dstGvfsPath the virtual destination path.
   * @return true if the rename is successful.
   * @throws IOException if an I/O error occurs.
   */
  public abstract boolean rename(Path srcGvfsPath, Path dstGvfsPath) throws IOException;

  /**
   * Delete a file or directory. Same as {@link FileSystem#delete(Path, boolean)}.
   *
   * @param gvfsPath the virtual path of the file or directory.
   * @param recursive whether to delete the directory recursively.
   * @return true if the deletion is successful.
   * @throws IOException if an I/O error occurs.
   */
  public abstract boolean delete(Path gvfsPath, boolean recursive) throws IOException;

  /**
   * Get the file status. Same as {@link FileSystem#getFileStatus(Path)}.
   *
   * @param gvfsPath the virtual path of the file.
   * @return the file status.
   * @throws IOException if an I/O error occurs.
   */
  public abstract FileStatus getFileStatus(Path gvfsPath) throws IOException;

  /**
   * List the statuses of the files/directories in the given path. Same as {@link
   * FileSystem#listStatus(Path)}.
   *
   * @param gvfsPath the virtual path of the directory.
   * @return the array of file statuses.
   * @throws IOException if an I/O error occurs.
   */
  public abstract FileStatus[] listStatus(Path gvfsPath) throws IOException;

  /**
   * Make the given file and all non-existent parents directories. Same as {@link
   * FileSystem#mkdirs(Path, FsPermission)}.
   *
   * @param gvfsPath the virtual path of the directory.
   * @param permission the permission.
   * @return true if the directory creation is successful.
   * @throws IOException if an I/O error occurs.
   */
  public abstract boolean mkdirs(Path gvfsPath, FsPermission permission) throws IOException;

  /**
   * Get the default replication factor for the file. Same as {@link
   * FileSystem#getDefaultReplication(Path)}.
   *
   * @param gvfsPath the virtual path of the file.
   * @return the default replication factor.
   */
  public abstract short getDefaultReplication(Path gvfsPath);

  /**
   * Get the default block size for the file. Same as {@link FileSystem#getDefaultBlockSize(Path)}.
   *
   * @param gvfsPath the virtual path of the file.
   * @return the default block size.
   */
  public abstract long getDefaultBlockSize(Path gvfsPath);

  /**
   * Add delegation tokens to the credentials. Same as {@link FileSystem#addDelegationTokens(String,
   * Credentials)}.
   *
   * @param renewer the renewer.
   * @param credentials the credentials.
   * @return the array of tokens.
   */
  public abstract Token<?>[] addDelegationTokens(String renewer, Credentials credentials);

  /**
   * Add delegation tokens for all file systems in the cache.
   *
   * @param renewer the renewer.
   * @param credentials the credentials.
   * @return the array of tokens.
   */
  protected Token<?>[] addDelegationTokensForAllFS(String renewer, Credentials credentials) {
    List<Token<?>> tokenList = Lists.newArrayList();
    for (FileSystem fileSystem : internalFileSystemCache.asMap().values()) {
      try {
        tokenList.addAll(Arrays.asList(fileSystem.addDelegationTokens(renewer, credentials)));
      } catch (IOException e) {
        LOG.warn("Failed to add delegation tokens for filesystem: {}", fileSystem.getUri(), e);
      }
    }
    return tokenList.stream().distinct().toArray(Token[]::new);
  }

  /**
   * Get the current location name. The subclass can override this method to provide a custom
   * location name.
   *
   * @return the current location name, or null if you want to use the default location.
   */
  @Nullable
  protected String currentLocationName() {
    return currentLocationName;
  }

  /**
   * Get the metalake name.
   *
   * @return the metalake name.
   */
  protected String metalakeName() {
    return metalakeName;
  }

  /**
   * Get the default block size.
   *
   * @return the default block size.
   */
  protected long defaultBlockSize() {
    return defaultBlockSize;
  }

  /**
   * Whether to enable credential vending.
   *
   * @return true if credential vending is enabled, false otherwise.
   */
  protected boolean enableCredentialVending() {
    return enableCredentialVending;
  }

  /**
   * Get the actual file path by the given virtual path and location name.
   *
   * @param gvfsPath the virtual path.
   * @param locationName the location name.
   * @param operation the fileset data operation.
   * @return the actual file path.
   * @throws FilesetPathNotFoundException if the fileset path is not found.
   */
  protected Path getActualFilePath(
      Path gvfsPath, String locationName, FilesetDataOperation operation)
      throws FilesetPathNotFoundException {
    NameIdentifier filesetIdent = extractIdentifier(metalakeName, gvfsPath.toString());
    String subPath = getSubPathFromGvfsPath(filesetIdent, gvfsPath.toString());
    NameIdentifier catalogIdent =
        NameIdentifier.of(filesetIdent.namespace().level(0), filesetIdent.namespace().level(1));
    String fileLocation;
    try {
      FilesetCatalog filesetCatalog = getFilesetCatalog(catalogIdent);
      setCallerContextForGetFileLocation(operation);
      fileLocation =
          filesetCatalog.getFileLocation(
              NameIdentifier.of(filesetIdent.namespace().level(2), filesetIdent.name()),
              subPath,
              locationName);
    } catch (NoSuchCatalogException | CatalogNotInUseException e) {
      String message = String.format("Cannot get fileset catalog by identifier: %s", catalogIdent);
      LOG.warn(message, e);
      throw new FilesetPathNotFoundException(message, e);

    } catch (NoSuchFilesetException e) {
      String message =
          String.format(
              "Cannot get fileset by fileset identifier: %s, sub_path %s", filesetIdent, subPath);
      LOG.warn(message, e);
      throw new FilesetPathNotFoundException(message, e);

    } catch (NoSuchLocationNameException e) {
      String message =
          String.format(
              "Location name not found by fileset identifier: %s, sub_path %s, location_name %s",
              filesetIdent, subPath, locationName);
      LOG.warn(message, e);
      throw new FilesetPathNotFoundException(message, e);
    }

    Path actualFilePath = new Path(fileLocation);
    URI uri = actualFilePath.toUri();
    String scheme = uri.getScheme();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(scheme), "Scheme of the actual file location cannot be null.");
    return new Path(fileLocation);
  }

  private void createFilesetLocationIfNeed(
      NameIdentifier filesetIdent, FileSystem fs, Path filesetPath) {
    NameIdentifier catalogIdent =
        NameIdentifier.of(filesetIdent.namespace().level(0), filesetIdent.namespace().level(1));
    // If the server-side filesystem ops are disabled, the fileset directory may not exist. In such
    // case the operations like create, open, list files under this directory will fail. So we
    // need to check the existence of the fileset directory beforehand.
    boolean fsOpsDisabled =
        ((Catalog) getFilesetCatalog(catalogIdent))
            .properties()
            .getOrDefault("disable-filesystem-ops", "false")
            .equalsIgnoreCase("true");
    if (fsOpsDisabled) {
      try {
        if (!fs.exists(filesetPath)) {
          fs.mkdirs(filesetPath);
          LOG.info(
              "Automatically created a directory for fileset path: {} when "
                  + "disable-filesystem-ops sets to true in the catalog properties.",
              filesetPath);
        }
      } catch (IOException e) {
        LOG.warn("Cannot create directory for fileset path: {}", filesetPath, e);
      }
    }
  }

  /**
   * Get the actual file system by the given virtual path.
   *
   * @param fileStatus the file status.
   * @param actualPrefix the actual prefix.
   * @param filesetPrefix the virtual prefix.
   * @return the file status with the converted path.
   */
  protected FileStatus convertFileStatusPathPrefix(
      FileStatus fileStatus, String actualPrefix, String filesetPrefix) {
    String filePath = fileStatus.getPath().toString();
    Preconditions.checkArgument(
        filePath.startsWith(actualPrefix),
        "Path %s doesn't start with prefix \"%s\".",
        filePath,
        actualPrefix);
    // if the storage location ends with "/",
    // we should truncate this to avoid replace issues.
    Path path =
        new Path(
            filePath.replaceFirst(
                actualPrefix.endsWith(SLASH) && !filesetPrefix.endsWith(SLASH)
                    ? actualPrefix.substring(0, actualPrefix.length() - 1)
                    : actualPrefix,
                filesetPrefix));
    fileStatus.setPath(path);

    return fileStatus;
  }

  /**
   * Get the virtual location by the given identifier.
   *
   * @param identifier the identifier.
   * @param withScheme whether to include the scheme.
   * @return the virtual location.
   */
  protected String getVirtualLocation(NameIdentifier identifier, boolean withScheme) {
    return String.format(
        "%s/%s/%s/%s",
        withScheme ? GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX : "",
        identifier.namespace().level(1),
        identifier.namespace().level(2),
        identifier.name());
  }

  /**
   * Get the actual file system corresponding to the given virtual path and location name.
   *
   * @param filesetPath the virtual path.
   * @param locationName the location name. null means the default location.
   * @return the actual file system.
   * @throws FilesetPathNotFoundException if the fileset path is not found.
   */
  protected FileSystem getActualFileSystem(Path filesetPath, String locationName)
      throws FilesetPathNotFoundException {
    NameIdentifier filesetIdent = extractIdentifier(metalakeName, filesetPath.toString());
    return getActualFileSystemByLocationName(filesetIdent, locationName);
  }

  /**
   * Get the fileset catalog by the catalog identifier from the cache or load it from the server if
   * the cache is disabled.
   *
   * @param catalogIdent the catalog identifier.
   * @return the fileset catalog.
   */
  protected FilesetCatalog getFilesetCatalog(NameIdentifier catalogIdent) {
    return filesetMetadataCache
        .map(cache -> cache.getFilesetCatalog(catalogIdent))
        .orElseGet(() -> gravitinoClient.loadCatalog(catalogIdent.name()).asFilesetCatalog());
  }

  /**
   * Get the fileset by the fileset identifier from the cache or load it from the server if the
   * cache is disabled.
   *
   * @param filesetIdent the fileset identifier.
   * @return the fileset.
   */
  protected Fileset getFileset(NameIdentifier filesetIdent) {
    return filesetMetadataCache
        .map(cache -> cache.getFileset(filesetIdent))
        .orElseGet(
            () ->
                getFilesetCatalog(
                        NameIdentifier.of(
                            filesetIdent.namespace().level(0), filesetIdent.namespace().level(1)))
                    .loadFileset(
                        NameIdentifier.of(filesetIdent.namespace().level(2), filesetIdent.name())));
  }

  @VisibleForTesting
  Cache<Pair<NameIdentifier, String>, FileSystem> internalFileSystemCache() {
    return internalFileSystemCache;
  }

  private void setCallerContextForGetFileLocation(FilesetDataOperation operation) {
    Map<String, String> contextMap = Maps.newHashMap();
    contextMap.put(
        FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
        InternalClientType.HADOOP_GVFS.name());
    contextMap.put(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION, operation.name());
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
    CallerContext.CallerContextHolder.set(callerContext);
  }

  private void setCallerContextForGetCredentials(String locationName) {
    Map<String, String> contextMap = Maps.newHashMap();
    contextMap.put(CredentialConstants.HTTP_HEADER_CURRENT_LOCATION_NAME, locationName);
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
    CallerContext.CallerContextHolder.set(callerContext);
  }

  private FileSystem getActualFileSystemByLocationName(
      NameIdentifier filesetIdent, String locationName) throws FilesetPathNotFoundException {
    NameIdentifier catalogIdent =
        NameIdentifier.of(filesetIdent.namespace().level(0), filesetIdent.namespace().level(1));
    try {
      return internalFileSystemCache.get(
          Pair.of(filesetIdent, locationName),
          cacheKey -> {
            try {
              Fileset fileset = getFileset(cacheKey.getLeft());
              String targetLocationName =
                  cacheKey.getRight() == null
                      ? fileset.properties().get(PROPERTY_DEFAULT_LOCATION_NAME)
                      : cacheKey.getRight();

              Preconditions.checkArgument(
                  fileset.storageLocations().containsKey(targetLocationName),
                  "Location name: %s is not found in fileset: %s.",
                  targetLocationName,
                  cacheKey.getLeft());

              Path targetLocation = new Path(fileset.storageLocations().get(targetLocationName));
              Map<String, String> allProperties =
                  getAllProperties(
                      cacheKey.getLeft(), targetLocation.toUri().getScheme(), targetLocationName);

              FileSystem actualFileSystem =
                  getActualFileSystemByPath(targetLocation, allProperties);
              createFilesetLocationIfNeed(cacheKey.getLeft(), actualFileSystem, targetLocation);
              return actualFileSystem;
            } catch (IOException ioe) {
              throw new GravitinoRuntimeException(
                  ioe,
                  "Exception occurs when create new FileSystem for fileset: %s, location: %s, msg: %s",
                  cacheKey.getLeft(),
                  cacheKey.getRight(),
                  ioe.getMessage());
            }
          });
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchCatalogException || cause instanceof CatalogNotInUseException) {
        String message =
            String.format("Cannot get fileset catalog by identifier: %s", catalogIdent);
        LOG.warn(message, e);
        throw new FilesetPathNotFoundException(message, e);
      }

      if (cause instanceof NoSuchFilesetException) {
        String message =
            String.format("Cannot get fileset by fileset identifier: %s", filesetIdent);
        LOG.warn(message, e);
        throw new FilesetPathNotFoundException(message, e);
      }

      if (cause instanceof NoSuchLocationNameException) {
        String message =
            String.format(
                "Location name not found by fileset identifier: %s, location_name %s",
                filesetIdent, locationName);
        LOG.warn(message, e);
        throw new FilesetPathNotFoundException(message, e);
      }

      throw e;
    }
  }

  private FileSystem getActualFileSystemByPath(
      Path actualFilePath, Map<String, String> allProperties) throws IOException {
    URI uri = actualFilePath.toUri();
    String scheme = uri.getScheme();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(scheme), "Scheme of the actual file location cannot be null.");

    FileSystemProvider provider = getFileSystemProviderByScheme(scheme);

    // Reset the FileSystem service loader to make sure the FileSystem will reload the
    // service file systems, this is a temporary solution to fix the issue
    // https://github.com/apache/gravitino/issues/5609
    resetFileSystemServiceLoader(scheme);

    return provider.getFileSystem(actualFilePath, allProperties);
  }

  private void resetFileSystemServiceLoader(String fsScheme) {
    try {
      Map<String, Class<? extends FileSystem>> serviceFileSystems =
          (Map<String, Class<? extends FileSystem>>)
              FieldUtils.getField(FileSystem.class, "SERVICE_FILE_SYSTEMS", true).get(null);

      if (serviceFileSystems.containsKey(fsScheme)) {
        return;
      }

      // Set this value to false so that FileSystem will reload the service file systems when
      // needed.
      FieldUtils.getField(FileSystem.class, "FILE_SYSTEMS_LOADED", true).set(null, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Cache<Pair<NameIdentifier, String>, FileSystem> newFileSystemCache(
      Configuration configuration) {
    int maxCapacity =
        configuration.getInt(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_DEFAULT);
    Preconditions.checkArgument(
        maxCapacity > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_FILESET_CACHE_MAX_CAPACITY_KEY);

    long evictionMillsAfterAccess =
        configuration.getLong(
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY,
            GravitinoVirtualFileSystemConfiguration
                .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_DEFAULT);
    Preconditions.checkArgument(
        evictionMillsAfterAccess > 0,
        "'%s' should be greater than 0",
        GravitinoVirtualFileSystemConfiguration
            .FS_GRAVITINO_FILESET_CACHE_EVICTION_MILLS_AFTER_ACCESS_KEY);

    Caffeine<Object, Object> cacheBuilder =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            // Since Caffeine does not ensure that removalListener will be involved after expiration
            // We use a scheduler with one thread to clean up expired fs.
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1, newDaemonThreadFactory("gvfs-filesystem-cache-cleaner"))))
            .removalListener(
                (key, value, cause) -> {
                  FileSystem fs = (FileSystem) value;
                  if (fs != null) {
                    try {
                      fs.close();
                    } catch (IOException e) {
                      LOG.error("Cannot close the file system for fileset: {}", key, e);
                    }
                  }
                });
    cacheBuilder.expireAfterAccess(evictionMillsAfterAccess, TimeUnit.MILLISECONDS);
    return cacheBuilder.build();
  }

  private Map<String, String> getAllProperties(
      NameIdentifier filesetIdent, String scheme, String locationName) {
    Catalog catalog =
        (Catalog)
            getFilesetCatalog(
                NameIdentifier.of(
                    filesetIdent.namespace().level(0), filesetIdent.namespace().level(1)));

    Map<String, String> allProperties = getNecessaryProperties(catalog.properties());
    allProperties.putAll(getConfigMap(conf));
    if (enableCredentialVending()) {
      allProperties.putAll(
          getCredentialProperties(
              getFileSystemProviderByScheme(scheme), filesetIdent, locationName));
    }
    return allProperties;
  }

  private Map<String, String> getNecessaryProperties(Map<String, String> properties) {
    return properties.entrySet().stream()
        .filter(property -> CATALOG_NECESSARY_PROPERTIES_TO_KEEP.contains(property.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<String, String> getCredentialProperties(
      FileSystemProvider fileSystemProvider,
      NameIdentifier filesetIdentifier,
      String locationName) {
    // Do not support credential vending, we do not need to add any credential properties.
    if (!(fileSystemProvider instanceof SupportsCredentialVending)) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
    try {
      Fileset fileset = getFileset(filesetIdentifier);
      setCallerContextForGetCredentials(locationName);
      Credential[] credentials = fileset.supportsCredentials().getCredentials();
      if (credentials.length > 0) {
        mapBuilder.put(
            GravitinoFileSystemCredentialsProvider.GVFS_CREDENTIAL_PROVIDER,
            DefaultGravitinoFileSystemCredentialsProvider.class.getCanonicalName());
        mapBuilder.put(
            GravitinoFileSystemCredentialsProvider.GVFS_NAME_IDENTIFIER,
            filesetIdentifier.toString());

        SupportsCredentialVending supportsCredentialVending =
            (SupportsCredentialVending) fileSystemProvider;
        mapBuilder.putAll(supportsCredentialVending.getFileSystemCredentialConf(credentials));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      CallerContext.CallerContextHolder.remove();
    }

    return mapBuilder.build();
  }

  private FileSystemProvider getFileSystemProviderByScheme(String scheme) {
    FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
    if (provider == null) {
      throw new GravitinoRuntimeException(
          "Unsupported file system scheme: %s for %s.",
          scheme, GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME);
    }
    return provider;
  }

  private ThreadFactory newDaemonThreadFactory(String name) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build();
  }

  private Map<String, FileSystemProvider> getFileSystemProviders() {
    Map<String, FileSystemProvider> resultMap = Maps.newHashMap();
    ServiceLoader<FileSystemProvider> allFileSystemProviders =
        ServiceLoader.load(FileSystemProvider.class);

    Streams.stream(allFileSystemProviders.iterator())
        .forEach(
            fileSystemProvider -> {
              if (resultMap.containsKey(fileSystemProvider.scheme())) {
                throw new UnsupportedOperationException(
                    String.format(
                        "File system provider: '%s' with scheme '%s' already exists in the provider list, "
                            + "please make sure the file system provider scheme is unique.",
                        fileSystemProvider.getClass().getName(), fileSystemProvider.scheme()));
              }
              resultMap.put(fileSystemProvider.scheme(), fileSystemProvider);
            });
    return resultMap;
  }

  private String initCurrentLocationName(Configuration configuration) {
    // get from configuration first, otherwise use the env variable
    // if both are not set, return null which means use the default location
    return Optional.ofNullable(configuration.get(FS_GRAVITINO_CURRENT_LOCATION_NAME))
        .orElse(System.getenv(currentLocationEnvVar));
  }
}
