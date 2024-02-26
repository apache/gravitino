/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.FilesetCatalog;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.datastrato.gravitino.shaded.org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
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

public class GravitinoVirtualFileSystem extends FileSystem {
  private Path workingDirectory;
  private URI uri;
  private GravitinoClient client;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private ConcurrentHashMap<NameIdentifier, FilesetInstance> identFilesetMapping =
      new ConcurrentHashMap<>();

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
              "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
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
    // Close the proxy fs cache, so that the user can not get the fs by FileSystem.get(),
    // avoid issues related to authority authentication
    configuration.set(String.format("fs.%s.impl.disable.cache", storageUri.getScheme()), "true");
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
    FilesetInstance instance = getOrCreateCachedFileset(identifier);
    FileSystem proxyFileSystem = instance.getFileSystem();
    FileStatus[] fileStatusResults = proxyFileSystem.listStatus(proxyPath);
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                resolveFileStatusPathScheme(
                    fileStatus,
                    instance.getFileset().storageLocation(),
                    concatFilesetPrefix(identifier, instance.getFileset().storageLocation())))
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
    FilesetInstance instance = getOrCreateCachedFileset(identifier);
    FileSystem proxyFileSystem = instance.getFileSystem();
    FileStatus fileStatus = proxyFileSystem.getFileStatus(proxyPath);
    return resolveFileStatusPathScheme(
        fileStatus,
        instance.getFileset().storageLocation(),
        concatFilesetPrefix(identifier, instance.getFileset().storageLocation()));
  }

  private String concatFilesetPrefix(NameIdentifier identifier, String storageLocation) {
    String filesetPrefix = GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX
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

  @VisibleForTesting
  Path resolvePathByIdentifier(NameIdentifier identifier, Path path) {
    String absolutePath = path.toString();
    if (absolutePath.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      try {
        FilesetInstance instance = getOrCreateCachedFileset(identifier);
        return new Path(
            absolutePath.replaceFirst(
                concatFilesetPrefix(identifier, instance.getFileset().storageLocation()),
                instance.getFileset().storageLocation()));
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

  private FilesetInstance getOrCreateCachedFileset(NameIdentifier identifier) throws IOException {
    try {
      rwLock.readLock().lock();
      FilesetInstance instance = identFilesetMapping.get(identifier);
      if (instance != null) {
        return instance;
      }
    } finally {
      rwLock.readLock().unlock();
    }

    try {
      rwLock.writeLock().lock();
      FilesetInstance instance = identFilesetMapping.get(identifier);
      if (instance != null) {
        return instance;
      }
      Fileset fileset = loadFileset(identifier);
      URI storageUri = URI.create(fileset.storageLocation());
      Configuration configuration = getConf();
      closeProxyFSCache(configuration, storageUri);
      FileSystem proxyFileSystem = FileSystem.get(storageUri, configuration);
      Preconditions.checkArgument(proxyFileSystem != null, "Cannot get the proxy file system");
      FilesetInstance filesetInstance =
          FilesetInstance.builder().withFileset(fileset).withFileSystem(proxyFileSystem).build();
      identFilesetMapping.putIfAbsent(identifier, filesetInstance);
      return filesetInstance;
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  private Fileset loadFileset(NameIdentifier filesetIdentifier) {
    GravitinoMetaLake metaLake =
        client.loadMetalake(NameIdentifier.of(filesetIdentifier.namespace().level(0)));
    Catalog catalog =
        metaLake.loadCatalog(
            NameIdentifier.of(
                filesetIdentifier.namespace().level(0), filesetIdentifier.namespace().level(1)));
    FilesetCatalog filesetCatalog = (FilesetCatalog) catalog.asFilesetCatalog();
    return filesetCatalog.loadFileset(filesetIdentifier);
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
    for (FilesetInstance instance : identFilesetMapping.values()) {
      try {
        instance.getFileSystem().close();
      } catch (IOException e) {
        // ignore
      }
    }
    super.close();
  }
}
