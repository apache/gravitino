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

public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger Logger = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private static final int DEFAULT_CACHE_CAPACITY = 20;
  private static final int CACHE_EXPIRE_AFTER_ACCESS_MINUTES = 5;
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
      Preconditions.checkArgument(
          StringUtils.isNotBlank(
              configuration.get(
                  GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY)),
          "Gravitino server uri is not set in the configuration");
      NameIdentifier filesetIdentifier = normalizedIdentifier(name.getPath());
      String serverUri =
          configuration.get(GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY);
      // TODO Need support more authentication types, now we only support simple auth
      this.client = GravitinoClient.builder(serverUri).withSimpleAuth().build();
      configuration.set(
          String.format(
              "fs.%s.impl.disable.filesetCache",
              GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
          "true");
      setConf(configuration);
      getOrCreateCachedFileset(filesetIdentifier);

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

  private void closeProxyFSCache(Configuration configuration, URI storageUri) {
    // Close the proxy fs filesetCache, so that the user can not get the fs by FileSystem.get(),
    // avoid issues related to authority authentication
    configuration.set(
        String.format("fs.%s.impl.disable.filesetCache", storageUri.getScheme()), "true");
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FileSystem proxyFileSystem = getOrCreateCachedFileset(identifier).getFileSystem();
    return proxyFileSystem.open(proxyPath, bufferSize);
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
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FileSystem proxyFileSystem = getOrCreateCachedFileset(identifier).getFileSystem();
    return proxyFileSystem.create(
        proxyPath, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FileSystem proxyFileSystem = getOrCreateCachedFileset(identifier).getFileSystem();
    return proxyFileSystem.append(proxyPath, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // Fileset identifier is not allowed to be renamed, only subdirectories can be renamed,
    // otherwise metadata may be inconsistent.
    NameIdentifier srcIdentifier = reservedIdentifier(src.toString());
    NameIdentifier dstIdentifier = reservedIdentifier(dst.toString());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path identifier should be same with src path identifier.");
    Path srcProxyPath = resolvePathByIdentifier(srcIdentifier, src);
    Path dstProxyPath = resolvePathByIdentifier(dstIdentifier, dst);
    FileSystem proxyFileSystem = getOrCreateCachedFileset(srcIdentifier).getFileSystem();
    return proxyFileSystem.rename(srcProxyPath, dstProxyPath);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FileSystem proxyFileSystem = getOrCreateCachedFileset(identifier).getFileSystem();
    return proxyFileSystem.delete(proxyPath, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FilesetMeta meta = getOrCreateCachedFileset(identifier);
    FileSystem proxyFileSystem = meta.getFileSystem();
    FileStatus[] fileStatusResults = proxyFileSystem.listStatus(proxyPath);
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                resolveFileStatusPathScheme(
                    fileStatus,
                    meta.getFileset().storageLocation(),
                    concatFilesetPrefix(identifier, meta.getFileset().storageLocation())))
        .toArray(FileStatus[]::new);
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    NameIdentifier identifier = reservedIdentifier(newDir.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, newDir);
    FileSystem proxyFileSystem = null;
    try {
      proxyFileSystem = getOrCreateCachedFileset(identifier).getFileSystem();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    proxyFileSystem.setWorkingDirectory(proxyPath);
    this.workingDirectory = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FileSystem proxyFileSystem = getOrCreateCachedFileset(identifier).getFileSystem();
    return proxyFileSystem.mkdirs(proxyPath, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    NameIdentifier identifier = reservedIdentifier(f.toString());
    Path proxyPath = resolvePathByIdentifier(identifier, f);
    FilesetMeta meta = getOrCreateCachedFileset(identifier);
    FileSystem proxyFileSystem = meta.getFileSystem();
    FileStatus fileStatus = proxyFileSystem.getFileStatus(proxyPath);
    return resolveFileStatusPathScheme(
        fileStatus,
        meta.getFileset().storageLocation(),
        concatFilesetPrefix(identifier, meta.getFileset().storageLocation()));
  }

  private String concatFilesetPrefix(NameIdentifier identifier, String storageLocation) {
    String filesetPrefix =
        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX
            + "/"
            + identifier.namespace().level(0)
            + "/"
            + identifier.namespace().level(1)
            + "/"
            + identifier.namespace().level(2)
            + "/"
            + identifier.name();
    if (storageLocation.endsWith("/")) {
      filesetPrefix += "/";
    }
    return filesetPrefix;
  }

  Path resolvePathByIdentifier(NameIdentifier identifier, Path path) {
    String absolutePath = path.toString();
    if (absolutePath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      try {
        FilesetMeta meta = getOrCreateCachedFileset(identifier);
        return new Path(
            absolutePath.replaceFirst(
                concatFilesetPrefix(identifier, meta.getFileset().storageLocation()),
                meta.getFileset().storageLocation()));
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Cannot resolve source path: %s to actual storage path", path));
      }
    } else {
      throw new InvalidPathException(
          String.format(
              "Path %s doesn't start with scheme \"%s\"",
              path, GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX));
    }
  }

  private FileStatus resolveFileStatusPathScheme(
      FileStatus fileStatus, String fromScheme, String toScheme) {
    String uri = fileStatus.getPath().toString();
    if (!uri.startsWith(fromScheme)) {
      throw new InvalidPathException(
          String.format("Path %s doesn't start with 'fromScheme' \"%s\"", uri, fromScheme));
    }
    String srcUri = uri.replaceFirst(fromScheme, toScheme);
    Path path = new Path(srcUri);
    fileStatus.setPath(path);
    return fileStatus;
  }

  private NameIdentifier reservedIdentifier(String path) {
    Preconditions.checkArgument(
        path.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX),
        "Path %s doesn't start with scheme \"%s\"");
    String reservedPath =
        path.substring((GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX).length());
    NameIdentifier reservedIdentifier = normalizedIdentifier(reservedPath);
    return reservedIdentifier;
  }

  private FilesetMeta getOrCreateCachedFileset(NameIdentifier identifier) throws IOException {
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
      Configuration configuration = getConf();
      closeProxyFSCache(configuration, storageUri);
      FileSystem proxyFileSystem = FileSystem.get(storageUri, configuration);
      Preconditions.checkArgument(proxyFileSystem != null, "Cannot get the proxy file system");
      meta = FilesetMeta.builder().withFileset(fileset).withFileSystem(proxyFileSystem).build();
      filesetCache.put(identifier, meta);
      return meta;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private Fileset loadFileset(NameIdentifier filesetIdentifier) {
    GravitinoMetaLake metalake =
        client.loadMetalake(NameIdentifier.of(filesetIdentifier.namespace().level(0)));
    Catalog catalog =
        metalake.loadCatalog(
            NameIdentifier.of(
                filesetIdentifier.namespace().level(0), filesetIdentifier.namespace().level(1)));
    return catalog.asFilesetCatalog().loadFileset(filesetIdentifier);
  }

  private NameIdentifier normalizedIdentifier(String path) {
    if (StringUtils.isBlank(path)) {
      throw new InvalidPathException("Path which need be normalized cannot be null or empty");
    }

    // remove first '/' symbol
    String[] reservedDirs = Arrays.stream(path.substring(1).split("/")).toArray(String[]::new);
    Preconditions.checkArgument(
        reservedDirs.length >= 4, "Path %s doesn't contains valid identifier", path);
    return NameIdentifier.of(reservedDirs[0], reservedDirs[1], reservedDirs[2], reservedDirs[3]);
  }

  @Override
  public void close() throws IOException {
    try {
      rwLock.writeLock().lock();
      for (FilesetMeta instance : filesetCache.asMap().values()) {
        try {
          instance.getFileSystem().close();
        } catch (IOException e) {
          // ignore
        }
      }
      filesetCache.invalidateAll();
    } finally {
      rwLock.writeLock().unlock();
    }
    super.close();
  }
}
