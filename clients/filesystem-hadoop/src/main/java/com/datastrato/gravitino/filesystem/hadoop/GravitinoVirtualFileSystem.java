/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.datastrato.gravitino.shaded.com.google.common.cache.Cache;
import com.datastrato.gravitino.shaded.com.google.common.cache.CacheBuilder;
import com.datastrato.gravitino.shaded.com.google.common.cache.RemovalListener;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Gravitino server, and creates an independent file system for it to act as an agent for users to
 * access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger Logger = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);
  private static final int DEFAULT_CACHE_CAPACITY = 20;
  private static final int CACHE_EXPIRE_AFTER_ACCESS_MINUTES = 5;
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private GravitinoMetaLake metalake;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  private final Cache<NameIdentifier, FilesetMeta> filesetCache =
      CacheBuilder.newBuilder()
          .maximumSize(DEFAULT_CACHE_CAPACITY)
          .expireAfterAccess(CACHE_EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
          .removalListener(
              (RemovalListener<NameIdentifier, FilesetMeta>)
                  removedCache -> {
                    if (removedCache.getKey() != null) {
                      try {
                        if (removedCache.getValue() != null) {
                          removedCache.getValue().getFileSystem().close();
                        }
                      } catch (IOException e) {
                        Logger.error(
                            String.format(
                                "Failed to close the file system for fileset: %s",
                                removedCache.getKey().toString()));
                      }
                    }
                  })
          .build();

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      // initialize the Gravitino client
      String serverUri =
          configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(serverUri),
          "Gravitino server uri is not set in the configuration");
      String metalakeName =
          configuration.get(
              GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(metalakeName),
          "Gravitino metalake is not set in the configuration");
      // TODO Need support more authentication types, now we only support simple auth
      this.client = GravitinoClient.builder(serverUri).withSimpleAuth().build();
      this.metalake = client.loadMetalake(NameIdentifier.ofMetalake(metalakeName));

      // Close the gvfs cache to achieve tenant isolation based on different user tokens in the
      // configuration.
      configuration.set(
          String.format(
              "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
          "true");
      setConf(configuration);

      NameIdentifier filesetIdentifier = extractIdentifier(name);
      getCachedFileset(filesetIdentifier);
      this.workingDirectory = new Path(name);
      this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());
      super.initialize(uri, getConf());
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s: ",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }
  }

  @VisibleForTesting
  Cache<NameIdentifier, FilesetMeta> getFilesetCache() {
    return filesetCache;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    return meta.getFileSystem().open(actualPath, bufferSize);
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    return meta.getFileSystem()
        .create(actualPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    return meta.getFileSystem().append(actualPath, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // There are two cases that cannot be renamed:
    // 1. Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    // 2. Fileset only mounts a single file, the storage location of the fileset cannot be renamed;
    // Otherwise the metadata in the Gravitino server may be inconsistent.
    NameIdentifier srcIdentifier = extractIdentifier(src.toUri());
    NameIdentifier dstIdentifier = extractIdentifier(dst.toUri());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);
    FilesetMeta meta = getCachedFileset(srcIdentifier);
    if (meta.getFileSystem().isFile(new Path(meta.getFileset().storageLocation()))) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot rename the fileset: %s which only mounts a single file.", srcIdentifier));
    } else {
      Path srcActualPath = resolvePathByIdentifier(srcIdentifier, meta, src);
      Path dstActualPath = resolvePathByIdentifier(dstIdentifier, meta, dst);
      return meta.getFileSystem().rename(srcActualPath, dstActualPath);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    return meta.getFileSystem().delete(actualPath, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    FileStatus[] fileStatusResults = meta.getFileSystem().listStatus(actualPath);
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                resolveFileStatusPathPrefix(
                    fileStatus,
                    meta.getFileset().storageLocation(),
                    concatFilesetPrefix(identifier, meta)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    try {
      NameIdentifier identifier = extractIdentifier(newDir.toUri());
      FilesetMeta meta = getCachedFileset(identifier);
      Path actualPath = resolvePathByIdentifier(identifier, meta, newDir);
      meta.getFileSystem().setWorkingDirectory(actualPath);
      this.workingDirectory = newDir;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    return meta.getFileSystem().mkdirs(actualPath, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    NameIdentifier identifier = extractIdentifier(f.toUri());
    FilesetMeta meta = getCachedFileset(identifier);
    Path actualPath = resolvePathByIdentifier(identifier, meta, f);
    FileStatus fileStatus = meta.getFileSystem().getFileStatus(actualPath);
    return resolveFileStatusPathPrefix(
        fileStatus, meta.getFileset().storageLocation(), concatFilesetPrefix(identifier, meta));
  }

  private String concatFilesetPrefix(NameIdentifier identifier, FilesetMeta meta) {
    String filesetPrefix =
        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX
            + identifier.namespace().level(1)
            + "/"
            + identifier.namespace().level(2)
            + "/"
            + identifier.name();
    if (meta.getFileset().storageLocation().endsWith("/")) {
      filesetPrefix += "/";
    }
    return filesetPrefix;
  }

  private Path resolvePathByIdentifier(NameIdentifier identifier, FilesetMeta meta, Path path) {
    String originPath = path.toString();
    if (originPath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      try {
        Path storageLocation = new Path(meta.getFileset().storageLocation());
        boolean isMountSingleFile = meta.getFileSystem().isFile(storageLocation);
        if (isMountSingleFile) {
          Preconditions.checkArgument(
              originPath.equals(concatFilesetPrefix(identifier, meta)),
              "Cannot resolve path: %s to actual storage path, because the fileset only mounts a single file.",
              path);
          return storageLocation;
        } else {
          return new Path(
              originPath.replaceFirst(
                  concatFilesetPrefix(identifier, meta), storageLocation.toString()));
        }
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Cannot resolve path: %s to actual storage path, exception:", path), e);
      }
    } else {
      throw new InvalidPathException(
          String.format(
              "Path %s doesn't start with the scheme \"%s\"",
              path, GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX));
    }
  }

  private FileStatus resolveFileStatusPathPrefix(
      FileStatus fileStatus, String fromPrefix, String toPrefix) {
    String uri = fileStatus.getPath().toString();
    if (!uri.startsWith(fromPrefix)) {
      throw new InvalidPathException(
          String.format("Path %s doesn't start with prefix \"%s\"", uri, fromPrefix));
    }
    String proxyUri = uri.replaceFirst(fromPrefix, toPrefix);
    Path path = new Path(proxyUri);
    fileStatus.setPath(path);
    return fileStatus;
  }

  private NameIdentifier extractIdentifier(URI uri) {
    Preconditions.checkArgument(
        uri.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX),
        "Path %s doesn't start with scheme \"%s\"");

    if (StringUtils.isBlank(uri.toString())) {
      throw new InvalidPathException("Uri which need be extracted cannot be null or empty");
    }

    // remove first '/' symbol with empty string
    String[] reservedDirs =
        Arrays.stream(uri.getPath().replaceFirst("/", "").split("/")).toArray(String[]::new);
    Preconditions.checkArgument(
        reservedDirs.length >= 3, "URI %s doesn't contains valid identifier", uri);

    return NameIdentifier.ofFileset(
        metalake.name(), reservedDirs[0], reservedDirs[1], reservedDirs[2]);
  }

  private FilesetMeta getCachedFileset(NameIdentifier identifier) throws IOException {
    try {
      rwLock.readLock().lock();
      FilesetMeta meta = filesetCache.getIfPresent(identifier);
      if (meta != null) {
        return meta;
      }
    } finally {
      rwLock.readLock().unlock();
    }

    try {
      rwLock.writeLock().lock();
      FilesetMeta meta = filesetCache.getIfPresent(identifier);
      if (meta != null) {
        return meta;
      }
      Fileset fileset = loadFileset(identifier);

      URI storageUri = URI.create(fileset.storageLocation());

      // Always create a new file system instance for the fileset.
      // Therefore, users cannot bypass gvfs and use `FileSystem.get()` to directly obtain the
      // Filesystem
      FileSystem actualFileSystem = FileSystem.newInstance(storageUri, getConf());
      Preconditions.checkState(actualFileSystem != null, "Cannot get the actual file system");

      meta = FilesetMeta.builder().withFileset(fileset).withFileSystem(actualFileSystem).build();

      filesetCache.put(identifier, meta);

      return meta;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private Fileset loadFileset(NameIdentifier identifier) {
    Catalog catalog =
        metalake.loadCatalog(
            NameIdentifier.ofCatalog(metalake.name(), identifier.namespace().level(1)));
    return catalog.asFilesetCatalog().loadFileset(identifier);
  }

  @Override
  public void close() throws IOException {
    try {
      rwLock.writeLock().lock();
      // close all actual FileSystems
      for (FilesetMeta instance : filesetCache.asMap().values()) {
        try {
          instance.getFileSystem().close();
        } catch (IOException e) {
          // ignore
        }
      }
      filesetCache.invalidateAll();
      // close the client
      try {
        if (client != null) {
          client.close();
        }
      } catch (Exception e) {
        // ignore
      }
    } finally {
      rwLock.writeLock().unlock();
    }

    super.close();
  }
}
