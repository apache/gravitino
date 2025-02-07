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
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
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
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Apache Gravitino server, and creates an independent file system for it to act as an agent for
 * users to access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger Logger = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private String metalakeName;
  private Cache<NameIdentifier, FilesetCatalog> catalogCache;
  private ScheduledThreadPoolExecutor catalogCleanScheduler;
  // Fileset name identifier and its corresponding FileSystem cache, the name identifier has
  // four levels, the first level is metalake name.
  private Cache<NameIdentifier, FileSystem> internalFileSystemCache;
  private ScheduledThreadPoolExecutor internalFileSystemCleanScheduler;

  // The pattern is used to match gvfs path. The scheme prefix (gvfs://fileset) is optional.
  // The following path can be match:
  //     gvfs://fileset/fileset_catalog/fileset_schema/fileset1/file.txt
  //     /fileset_catalog/fileset_schema/fileset1/sub_dir/
  private static final Pattern IDENTIFIER_PATTERN =
      Pattern.compile("^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$");
  private static final String SLASH = "/";
  private final Map<String, FileSystemProvider> fileSystemProvidersMap = Maps.newHashMap();

  private static final Set<String> CATALOG_NECESSARY_PROPERTIES_TO_KEEP =
      Sets.newHashSet(
          OSSProperties.GRAVITINO_OSS_ENDPOINT,
          OSSProperties.GRAVITINO_OSS_REGION,
          S3Properties.GRAVITINO_S3_ENDPOINT,
          S3Properties.GRAVITINO_S3_REGION,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }

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

    initializeFileSystemCache(maxCapacity, evictionMillsAfterAccess);
    initializeCatalogCache();

    this.metalakeName =
        configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "'%s' is not set in the configuration",
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);

    this.client = GravitinoVirtualFileSystemUtils.createClient(configuration);
    // Register the default local and HDFS FileSystemProvider
    fileSystemProvidersMap.putAll(getFileSystemProviders());

    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  @VisibleForTesting
  Cache<NameIdentifier, FileSystem> internalFileSystemCache() {
    return internalFileSystemCache;
  }

  private void initializeFileSystemCache(int maxCapacity, long expireAfterAccess) {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.internalFileSystemCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-filesystem-cache-cleaner"));
    Caffeine<Object, Object> cacheBuilder =
        Caffeine.newBuilder()
            .maximumSize(maxCapacity)
            .scheduler(Scheduler.forScheduledExecutorService(internalFileSystemCleanScheduler))
            .removalListener(
                (key, value, cause) -> {
                  FileSystem fs = (FileSystem) value;
                  if (fs != null) {
                    try {
                      fs.close();
                    } catch (IOException e) {
                      Logger.error("Cannot close the file system for fileset: {}", key, e);
                    }
                  }
                });
    if (expireAfterAccess > 0) {
      cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
    }
    this.internalFileSystemCache = cacheBuilder.build();
  }

  private void initializeCatalogCache() {
    // Since Caffeine does not ensure that removalListener will be involved after expiration
    // We use a scheduler with one thread to clean up expired clients.
    this.catalogCleanScheduler =
        new ScheduledThreadPoolExecutor(1, newDaemonThreadFactory("gvfs-catalog-cache-cleaner"));
    // In most scenarios, it will not read so many catalog filesets at the same time, so we can just
    // set a default value for this cache.
    this.catalogCache =
        Caffeine.newBuilder()
            .maximumSize(100)
            .scheduler(Scheduler.forScheduledExecutorService(catalogCleanScheduler))
            .build();
  }

  private ThreadFactory newDaemonThreadFactory(String name) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(name + "-%d").build();
  }

  private String getVirtualLocation(NameIdentifier identifier, boolean withScheme) {
    return String.format(
        "%s/%s/%s/%s",
        withScheme ? GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX : "",
        identifier.namespace().level(1),
        identifier.namespace().level(2),
        identifier.name());
  }

  @VisibleForTesting
  FileStatus convertFileStatusPathPrefix(
      FileStatus fileStatus, String actualPrefix, String virtualPrefix) {
    String filePath = fileStatus.getPath().toString();
    Preconditions.checkArgument(
        filePath.startsWith(actualPrefix),
        "Path %s doesn't start with prefix \"%s\".",
        filePath,
        actualPrefix);
    // if the storage location is end with "/",
    // we should truncate this to avoid replace issues.
    Path path =
        new Path(
            filePath.replaceFirst(
                actualPrefix.endsWith(SLASH) && !virtualPrefix.endsWith(SLASH)
                    ? actualPrefix.substring(0, actualPrefix.length() - 1)
                    : actualPrefix,
                virtualPrefix));
    fileStatus.setPath(path);

    return fileStatus;
  }

  @VisibleForTesting
  NameIdentifier extractIdentifier(URI virtualUri) {
    String virtualPath = virtualUri.toString();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(virtualPath),
        "Uri which need be extracted cannot be null or empty.");

    Matcher matcher = IDENTIFIER_PATTERN.matcher(virtualPath);
    Preconditions.checkArgument(
        matcher.matches() && matcher.groupCount() == 3,
        "URI %s doesn't contains valid identifier",
        virtualPath);

    return NameIdentifier.of(metalakeName, matcher.group(1), matcher.group(2), matcher.group(3));
  }

  private FilesetContextPair getFilesetContext(Path virtualPath, FilesetDataOperation operation) {
    NameIdentifier identifier = extractIdentifier(virtualPath.toUri());
    String virtualPathString = virtualPath.toString();
    String subPath = getSubPathFromVirtualPath(identifier, virtualPathString);

    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, identifier.namespace().level(1));
    FilesetCatalog filesetCatalog =
        catalogCache.get(
            catalogIdent, ident -> client.loadCatalog(catalogIdent.name()).asFilesetCatalog());
    Catalog catalog = (Catalog) filesetCatalog;
    Preconditions.checkArgument(
        filesetCatalog != null, String.format("Loaded fileset catalog: %s is null.", catalogIdent));

    Map<String, String> contextMap = Maps.newHashMap();
    contextMap.put(
        FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
        InternalClientType.HADOOP_GVFS.name());
    contextMap.put(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION, operation.name());
    CallerContext callerContext = CallerContext.builder().withContext(contextMap).build();
    CallerContext.CallerContextHolder.set(callerContext);

    String actualFileLocation =
        filesetCatalog.getFileLocation(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name()), subPath);

    Path filePath = new Path(actualFileLocation);
    URI uri = filePath.toUri();
    // we cache the fs for the same scheme, so we can reuse it
    String scheme = uri.getScheme();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(scheme), "Scheme of the actual file location cannot be null.");
    FileSystem fs =
        internalFileSystemCache.get(
            identifier,
            ident -> {
              try {
                FileSystemProvider provider = fileSystemProvidersMap.get(scheme);
                if (provider == null) {
                  throw new GravitinoRuntimeException(
                      "Unsupported file system scheme: %s for %s.",
                      scheme, GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME);
                }

                // Reset the FileSystem service loader to make sure the FileSystem will reload the
                // service file systems, this is a temporary solution to fix the issue
                // https://github.com/apache/gravitino/issues/5609
                resetFileSystemServiceLoader(scheme);

                Map<String, String> necessaryPropertyFromCatalog =
                    catalog.properties().entrySet().stream()
                        .filter(
                            property ->
                                CATALOG_NECESSARY_PROPERTIES_TO_KEEP.contains(property.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                Map<String, String> totalProperty = Maps.newHashMap(necessaryPropertyFromCatalog);
                totalProperty.putAll(getConfigMap(getConf()));

                totalProperty.putAll(getCredentialProperties(provider, catalog, identifier));

                return provider.getFileSystem(filePath, totalProperty);
              } catch (IOException ioe) {
                throw new GravitinoRuntimeException(
                    ioe,
                    "Exception occurs when create new FileSystem for actual uri: %s, msg: %s",
                    uri,
                    ioe.getMessage());
              }
            });

    return new FilesetContextPair(new Path(actualFileLocation), fs);
  }

  private Map<String, String> getCredentialProperties(
      FileSystemProvider fileSystemProvider, Catalog catalog, NameIdentifier filesetIdentifier) {
    // Do not support credential vending, we do not need to add any credential properties.
    if (!(fileSystemProvider instanceof SupportsCredentialVending)) {
      return ImmutableMap.of();
    }

    ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
    try {
      Fileset fileset =
          catalog
              .asFilesetCatalog()
              .loadFileset(
                  NameIdentifier.of(
                      filesetIdentifier.namespace().level(2), filesetIdentifier.name()));
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
    }

    return mapBuilder.build();
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

  private Map<String, String> getConfigMap(Configuration configuration) {
    Map<String, String> maps = Maps.newHashMap();
    configuration.forEach(entry -> maps.put(entry.getKey(), entry.getValue()));
    return maps;
  }

  private String getSubPathFromVirtualPath(NameIdentifier identifier, String virtualPathString) {
    return virtualPathString.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)
        ? virtualPathString.substring(
            String.format(
                    "%s/%s/%s/%s",
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX,
                    identifier.namespace().level(1),
                    identifier.namespace().level(2),
                    identifier.name())
                .length())
        : virtualPathString.substring(
            String.format(
                    "/%s/%s/%s",
                    identifier.namespace().level(1),
                    identifier.namespace().level(2),
                    identifier.name())
                .length());
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public synchronized void setWorkingDirectory(Path newDir) {
    FilesetContextPair context = getFilesetContext(newDir, FilesetDataOperation.SET_WORKING_DIR);
    context.getFileSystem().setWorkingDirectory(context.getActualFileLocation());
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.OPEN);
    return context.getFileSystem().open(context.getActualFileLocation(), bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.CREATE);
    return context
        .getFileSystem()
        .create(
            context.getActualFileLocation(),
            permission,
            overwrite,
            bufferSize,
            replication,
            blockSize,
            progress);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.APPEND);
    return context.getFileSystem().append(context.getActualFileLocation(), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    NameIdentifier srcIdentifier = extractIdentifier(src.toUri());
    NameIdentifier dstIdentifier = extractIdentifier(dst.toUri());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);

    FilesetContextPair srcContext = getFilesetContext(src, FilesetDataOperation.RENAME);
    FilesetContextPair dstContext = getFilesetContext(dst, FilesetDataOperation.RENAME);

    return srcContext
        .getFileSystem()
        .rename(srcContext.getActualFileLocation(), dstContext.getActualFileLocation());
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.DELETE);
    return context.getFileSystem().delete(context.getActualFileLocation(), recursive);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.GET_FILE_STATUS);
    FileStatus fileStatus = context.getFileSystem().getFileStatus(context.getActualFileLocation());
    NameIdentifier identifier = extractIdentifier(path.toUri());
    String subPath = getSubPathFromVirtualPath(identifier, path.toString());
    String storageLocation =
        context
            .getActualFileLocation()
            .toString()
            .substring(0, context.getActualFileLocation().toString().length() - subPath.length());
    return convertFileStatusPathPrefix(
        fileStatus, storageLocation, getVirtualLocation(identifier, true));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.LIST_STATUS);
    FileStatus[] fileStatusResults =
        context.getFileSystem().listStatus(context.getActualFileLocation());
    NameIdentifier identifier = extractIdentifier(path.toUri());
    String subPath = getSubPathFromVirtualPath(identifier, path.toString());
    String storageLocation =
        context
            .getActualFileLocation()
            .toString()
            .substring(0, context.getActualFileLocation().toString().length() - subPath.length());
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                convertFileStatusPathPrefix(
                    fileStatus, storageLocation, getVirtualLocation(identifier, true)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    FilesetContextPair context = getFilesetContext(path, FilesetDataOperation.MKDIRS);
    return context.getFileSystem().mkdirs(context.getActualFileLocation(), permission);
  }

  @Override
  public short getDefaultReplication(Path f) {
    FilesetContextPair context = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_REPLICATION);
    return context.getFileSystem().getDefaultReplication(context.getActualFileLocation());
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    FilesetContextPair context = getFilesetContext(f, FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
    return context.getFileSystem().getDefaultBlockSize(context.getActualFileLocation());
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
    List<Token<?>> tokenList = Lists.newArrayList();
    for (FileSystem fileSystem : internalFileSystemCache.asMap().values()) {
      try {
        tokenList.addAll(Arrays.asList(fileSystem.addDelegationTokens(renewer, credentials)));
      } catch (IOException e) {
        Logger.warn("Failed to add delegation tokens for filesystem: {}", fileSystem.getUri(), e);
      }
    }
    return tokenList.stream().distinct().toArray(Token[]::new);
  }

  @Override
  public synchronized void close() throws IOException {
    // close all actual FileSystems
    for (FileSystem fileSystem : internalFileSystemCache.asMap().values()) {
      try {
        fileSystem.close();
      } catch (IOException e) {
        // ignore
      }
    }
    internalFileSystemCache.invalidateAll();
    catalogCache.invalidateAll();
    // close the client
    try {
      if (client != null) {
        client.close();
      }
    } catch (Exception e) {
      // ignore
    }
    catalogCleanScheduler.shutdownNow();
    internalFileSystemCleanScheduler.shutdownNow();
    super.close();
  }

  private static class FilesetContextPair {
    private final Path actualFileLocation;
    private final FileSystem fileSystem;

    public FilesetContextPair(Path actualFileLocation, FileSystem fileSystem) {
      this.actualFileLocation = actualFileLocation;
      this.fileSystem = fileSystem;
    }

    public Path getActualFileLocation() {
      return actualFileLocation;
    }

    public FileSystem getFileSystem() {
      return fileSystem;
    }
  }

  private static Map<String, FileSystemProvider> getFileSystemProviders() {
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
}
