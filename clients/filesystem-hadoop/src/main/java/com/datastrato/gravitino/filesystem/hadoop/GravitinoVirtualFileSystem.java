/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
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
  private FileSystem proxyFileSystem;
  private Path workingDirectory;
  private URI uri;
  private String filesetIdentifier;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    if (name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      try {
        // TODO We will replace the default uri to get the truly file system when we
        //  can interact with gravitino server, now we only support hdfs scheme like
        //  'hdfs://${host}'
        URI defaultUri = FileSystem.getDefaultUri(conf);
        URI newUri =
            new URI(
                GravitinoVirtualFileSystemConfiguration.HDFS_SCHEME
                    + "://"
                    + defaultUri.getAuthority());
        // close cache, so that single fileset can use single fs
        conf.set(
            String.format(
                "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME),
            "true");
        conf.set(
            String.format(
                "fs.%s.impl.disable.cache", GravitinoVirtualFileSystemConfiguration.HDFS_SCHEME),
            "true");
        setConf(conf);
        this.proxyFileSystem = FileSystem.get(newUri, conf);
        this.workingDirectory = new Path(name);
        this.uri = name;
        // /${metalake}/${catalog}/${schema}/${fileset}
        this.filesetIdentifier = normalizedIdentifier(name.getPath());
      } catch (URISyntaxException e) {
        throw new RuntimeException(
            String.format("Cannot create proxy file system uri: %s, exception: %s", name, e));
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s: ",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return proxyFileSystem.open(resolvePathScheme(f), bufferSize);
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
    return proxyFileSystem.create(
        resolvePathScheme(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return proxyFileSystem.append(resolvePathScheme(f), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return proxyFileSystem.rename(resolvePathScheme(src), resolvePathScheme(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return proxyFileSystem.delete(resolvePathScheme(f), recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] fileStatusResults = proxyFileSystem.listStatus(resolvePathScheme(f));
    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                resolveFileStatusPathScheme(
                    fileStatus,
                    proxyFileSystem.getScheme() + "://" + proxyFileSystem.getUri().getHost(),
                    GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX))
        .toArray(FileStatus[]::new);
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    proxyFileSystem.setWorkingDirectory(resolvePathScheme(newDir));
    this.workingDirectory = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return proxyFileSystem.mkdirs(resolvePathScheme(f), permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus fileStatus = proxyFileSystem.getFileStatus(resolvePathScheme(f));
    return resolveFileStatusPathScheme(
        fileStatus,
        proxyFileSystem.getScheme() + "://" + proxyFileSystem.getUri().getHost(),
        GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX);
  }

  private Path resolvePathScheme(Path path) {
    return resolvePathScheme(path, proxyFileSystem);
  }

  private Path resolvePathScheme(Path path, FileSystem fileSystem) {
    checkPathValid(path);
    URI uri = path.toUri();
    if (uri.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      try {
        URI newUri =
            new URI(
                fileSystem.getScheme(),
                uri.getUserInfo(),
                fileSystem.getUri().getHost(),
                fileSystem.getUri().getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment());
        return new Path(newUri);
      } catch (URISyntaxException e) {
        throw new RuntimeException(
            String.format("Cannot resolve source path: %s to actual storage path", path));
      }
    }
    return path;
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

  private void checkPathValid(Path path) {
    String uri = path.toString();
    if (!uri.startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new InvalidPathException(
          String.format(
              "Path %s doesn't start with scheme \"%s\"",
              uri, GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX));
    }
    String reservedUri =
        uri.substring(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX.length());
    String reservedIdentifier = normalizedIdentifier(reservedUri);
    if (!reservedIdentifier.equals(filesetIdentifier)) {
      throw new InvalidPathException(
          String.format(
              "Path %s doesn't contains valid identifier \"%s\"", path, filesetIdentifier));
    }
  }

  private String normalizedIdentifier(String path) {
    if (path == null || path.length() == 0) {
      throw new InvalidPathException("Path which need be normalized cannot be null or empty");
    }

    // remove first '/' symbol
    String[] reservedDirs = Arrays.stream(path.substring(1).split("/")).toArray(String[]::new);
    if (reservedDirs.length >= 4) {
      return String.format(
          "/%s/%s/%s/%s", reservedDirs[0], reservedDirs[1], reservedDirs[2], reservedDirs[3]);
    } else {
      throw new InvalidPathException(
          String.format(
              "Path %s doesn't contains valid identifier \"%s\"", path, filesetIdentifier));
    }
  }
}
