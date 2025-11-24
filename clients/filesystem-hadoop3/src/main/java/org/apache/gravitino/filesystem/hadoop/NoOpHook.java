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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * The default implementation of {@link GravitinoVirtualFileSystemHook}. This class does nothing.
 */
public class NoOpHook implements GravitinoVirtualFileSystemHook {

  @Override
  public void initialize(Map<String, String> config) {}

  @Override
  public Path preSetWorkingDirectory(Path path) {
    return path;
  }

  @Override
  public void postSetWorkingDirectory(Path gvfsPath) {}

  @Override
  public Path preOpen(Path path, int bufferSize) {
    return path;
  }

  @Override
  public FSDataInputStream postOpen(Path gvfsPath, int bufferSize, FSDataInputStream inputStream) {
    return inputStream;
  }

  @Override
  public Path preCreate(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize) {
    return path;
  }

  @Override
  public FSDataOutputStream postCreate(
      Path gvfsPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      FSDataOutputStream outputStream) {
    return outputStream;
  }

  @Override
  public Path preAppend(Path path, int bufferSize) {
    return path;
  }

  @Override
  public FSDataOutputStream postAppend(
      Path gvfsPath, int bufferSize, FSDataOutputStream outputStream) {
    return outputStream;
  }

  @Override
  public Pair<Path, Path> preRename(Path src, Path dst) {
    return Pair.of(src, dst);
  }

  @Override
  public boolean postRename(Path srcGvfsPath, Path dstGvfsPath, boolean success) {
    return success;
  }

  @Override
  public Path preDelete(Path path, boolean recursive) {
    return path;
  }

  @Override
  public boolean postDelete(Path gvfsPath, boolean recursive, boolean success) {
    return success;
  }

  @Override
  public Path preGetFileStatus(Path path) {
    return path;
  }

  @Override
  public FileStatus postGetFileStatus(FileStatus fileStatus) {
    return fileStatus;
  }

  @Override
  public Path preListStatus(Path path) {
    return path;
  }

  @Override
  public FileStatus[] postListStatus(FileStatus[] fileStatuses) {
    return fileStatuses;
  }

  @Override
  public Path preMkdirs(Path path, FsPermission permission) {
    return path;
  }

  @Override
  public boolean postMkdirs(Path gvfsPath, FsPermission permission, boolean success) {
    return success;
  }

  @Override
  public Path preGetDefaultReplication(Path path) {
    return path;
  }

  @Override
  public short postGetDefaultReplication(Path gvfsPath, short replication) {
    return replication;
  }

  @Override
  public Path preGetDefaultBlockSize(Path path) {
    return path;
  }

  @Override
  public long postGetDefaultBlockSize(Path gvfsPath, long blockSize) {
    return blockSize;
  }

  @Override
  public void onSetWorkingDirectoryFailure(Path path, Exception e) {
    throw asUnchecked(e);
  }

  @Override
  public FSDataInputStream onOpenFailure(Path path, int bufferSize, Exception e)
      throws IOException {
    if (e instanceof IOException) {
      throw (IOException) e;
    }
    throw asUnchecked(e);
  }

  @Override
  public FSDataOutputStream onCreateFailure(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      Exception e)
      throws IOException {
    if (e instanceof NoSuchCatalogException
        || e instanceof CatalogNotInUseException
        || e instanceof NoSuchFilesetException
        || e instanceof NoSuchLocationNameException) {
      String message =
          "Fileset is not found for path: "
              + path
              + " for operation CREATE. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }

    if (e instanceof IOException) {
      throw (IOException) e;
    }

    throw asUnchecked(e);
  }

  @Override
  public FSDataOutputStream onAppendFailure(
      Path path, int bufferSize, Progressable progress, Exception e) throws IOException {
    if (e instanceof IOException) {
      throw (IOException) e;
    }

    throw asUnchecked(e);
  }

  @Override
  public boolean onRenameFailure(Path src, Path dst, Exception e) throws IOException {
    if (e instanceof IOException) {
      throw (IOException) e;
    }
    throw asUnchecked(e);
  }

  @Override
  public boolean onDeleteFailure(Path path, boolean recursive, Exception e) throws IOException {
    if (e instanceof FileNotFoundException) {
      return false;
    }

    if (e instanceof IOException) {
      throw (IOException) e;
    }

    throw asUnchecked(e);
  }

  @Override
  public FileStatus onGetFileStatusFailure(Path path, Exception e) throws IOException {
    if (e instanceof IOException) {
      throw (IOException) e;
    }
    throw asUnchecked(e);
  }

  @Override
  public FileStatus[] onListStatusFailure(Path path, Exception e) throws IOException {
    if (e instanceof IOException) {
      throw (IOException) e;
    }
    throw asUnchecked(e);
  }

  @Override
  public boolean onMkdirsFailure(Path path, FsPermission permission, Exception e)
      throws IOException {
    if (e instanceof NoSuchCatalogException
        || e instanceof CatalogNotInUseException
        || e instanceof NoSuchFilesetException
        || e instanceof NoSuchLocationNameException) {
      String message =
          "Fileset is not found for path: "
              + path
              + " for operation MKDIRS. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }

    if (e instanceof IOException) {
      throw (IOException) e;
    }

    throw asUnchecked(e);
  }

  @Override
  public short onGetDefaultReplicationFailure(Path path, Exception e) {
    if (e instanceof IOException) {
      return 1;
    }

    throw asUnchecked(e);
  }

  @Override
  public long onGetDefaultBlockSizeFailure(Path f, Exception e, long defaultBlockSize) {
    if (e instanceof IOException) {
      return defaultBlockSize;
    }

    throw asUnchecked(e);
  }

  @Override
  public void close() throws IOException {}
}
