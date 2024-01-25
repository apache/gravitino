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

public class GravitinoFileSystem extends FileSystem {
  private FileSystem proxyFileSystem;
  private Path workingDirectory;
  private URI uri;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    if (name.toString().startsWith(GravitinoFileSystemConfiguration.GTFS_SCHEME_PREFIX)) {
      // TODO We will interact with gravitino server to get the storage location,
      //  then we can get the truly file system; now we only support hdfs://
      try {
        URI newUri =
            new URI(
                GravitinoFileSystemConfiguration.HDFS_SCHEME,
                name.getUserInfo(),
                name.getHost(),
                name.getPort(),
                name.getPath(),
                name.getQuery(),
                name.getFragment());
        this.proxyFileSystem = FileSystem.get(newUri, conf);
        this.workingDirectory = new Path(name);
        this.uri = name;
      } catch (URISyntaxException e) {
        throw new RuntimeException(
            String.format("Cannot create proxy file system uri: %s, exception: %s", name, e));
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system protocol: %s for %s: ",
              name.getScheme(), GravitinoFileSystemConfiguration.GTFS_SCHEME));
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
                    proxyFileSystem.getScheme(),
                    GravitinoFileSystemConfiguration.GTFS_SCHEME))
        .toArray(FileStatus[]::new);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    proxyFileSystem.setWorkingDirectory(resolvePathScheme(new_dir));
    this.workingDirectory = new_dir;
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
        fileStatus, proxyFileSystem.getScheme(), GravitinoFileSystemConfiguration.GTFS_SCHEME);
  }

  private Path resolvePathScheme(Path path) {
    return resolvePathScheme(path, proxyFileSystem.getScheme());
  }

  private Path resolvePathScheme(Path path, String scheme) {
    URI uri = path.toUri();
    if (uri.toString().startsWith(GravitinoFileSystemConfiguration.GTFS_SCHEME_PREFIX)) {
      try {
        URI newUri =
            new URI(
                scheme,
                uri.getUserInfo(),
                uri.getHost(),
                uri.getPort(),
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

  private static FileStatus resolveFileStatusPathScheme(
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
}
