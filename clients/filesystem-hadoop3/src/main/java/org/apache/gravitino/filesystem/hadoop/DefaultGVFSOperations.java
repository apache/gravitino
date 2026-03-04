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

import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.extractIdentifier;
import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getSubPathFromGvfsPath;

import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.FilesetDataOperation;
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

/**
 * The default implementation of {@link BaseGVFSOperations} which takes operations on the default
 * location of the fileset if the location name is not provided in both configuration and the
 * environment.
 */
public class DefaultGVFSOperations extends BaseGVFSOperations {

  /**
   * Constructs a new {@link DefaultGVFSOperations} with the given {@link Configuration}.
   *
   * @param configuration the configuration
   */
  public DefaultGVFSOperations(Configuration configuration) {
    super(configuration);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public FSDataInputStream open(Path gvfsPath, int bufferSize) throws IOException {
    FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
    Path actualFilePath =
        getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.OPEN);
    return actualFs.open(actualFilePath, bufferSize);
  }

  @Override
  public synchronized void setWorkingDirectory(Path gvfsDir) throws FileNotFoundException {
    FileSystem actualFs = getActualFileSystem(gvfsDir, currentLocationName());
    Path actualFilePath =
        getActualFilePath(gvfsDir, currentLocationName(), FilesetDataOperation.SET_WORKING_DIR);
    actualFs.setWorkingDirectory(actualFilePath);
  }

  @Override
  public FSDataOutputStream create(
      Path gvfsPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    try {
      FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
      Path actualFilePath =
          getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.CREATE);
      return actualFs.create(
          actualFilePath, permission, overwrite, bufferSize, replication, blockSize, progress);
    } catch (FileNotFoundException e) {
      String message =
          "Fileset is not found for path: "
              + gvfsPath
              + " for operation CREATE. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }
  }

  @Override
  public FSDataOutputStream append(Path gvfsPath, int bufferSize, Progressable progress)
      throws IOException {
    FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
    Path actualFilePath =
        getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.APPEND);
    return actualFs.append(actualFilePath, bufferSize, progress);
  }

  @Override
  public boolean rename(Path srcGvfsPath, Path dstGvfsPath) throws IOException {
    // Fileset identifier is not allowed to be renamed, only its subdirectories can be renamed
    // which not in the storage location of the fileset;
    NameIdentifier srcIdentifier = extractIdentifier(metalakeName(), srcGvfsPath.toString());
    NameIdentifier dstIdentifier = extractIdentifier(metalakeName(), dstGvfsPath.toString());
    Preconditions.checkArgument(
        srcIdentifier.equals(dstIdentifier),
        "Destination path fileset identifier: %s should be same with src path "
            + "fileset identifier: %s.",
        srcIdentifier,
        dstIdentifier);

    Path srcActualPath =
        getActualFilePath(srcGvfsPath, currentLocationName(), FilesetDataOperation.RENAME);
    Path dstActualPath =
        getActualFilePath(dstGvfsPath, currentLocationName(), FilesetDataOperation.RENAME);
    FileSystem actualFs = getActualFileSystem(srcGvfsPath, currentLocationName());
    return actualFs.rename(srcActualPath, dstActualPath);
  }

  @Override
  public boolean delete(Path gvfsPath, boolean recursive) throws IOException {
    try {
      FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
      Path actualFilePath =
          getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.DELETE);
      return actualFs.delete(actualFilePath, recursive);
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path gvfsPath) throws IOException {
    FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
    Path actualFilePath =
        getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.GET_FILE_STATUS);
    FileStatus fileStatus = actualFs.getFileStatus(actualFilePath);

    NameIdentifier identifier = extractIdentifier(metalakeName(), gvfsPath.toString());
    String subPath = getSubPathFromGvfsPath(identifier, gvfsPath.toString());
    String filesetLocation =
        actualFilePath
            .toString()
            .substring(0, actualFilePath.toString().length() - subPath.length());

    return convertFileStatusPathPrefix(
        fileStatus, filesetLocation, getVirtualLocation(identifier, true));
  }

  @Override
  public FileStatus[] listStatus(Path gvfsPath) throws IOException {
    FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
    Path actualFilePath =
        getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.LIST_STATUS);
    FileStatus[] fileStatusResults = actualFs.listStatus(actualFilePath);

    NameIdentifier identifier = extractIdentifier(metalakeName(), gvfsPath.toString());
    String subPath = getSubPathFromGvfsPath(identifier, gvfsPath.toString());
    String filesetLocation =
        actualFilePath
            .toString()
            .substring(0, actualFilePath.toString().length() - subPath.length());

    return Arrays.stream(fileStatusResults)
        .map(
            fileStatus ->
                convertFileStatusPathPrefix(
                    fileStatus, filesetLocation, getVirtualLocation(identifier, true)))
        .toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path gvfsPath, FsPermission permission) throws IOException {
    try {
      FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
      Path actualFilePath =
          getActualFilePath(gvfsPath, currentLocationName(), FilesetDataOperation.MKDIRS);
      return actualFs.mkdirs(actualFilePath, permission);
    } catch (FileNotFoundException e) {
      String message =
          "Fileset is not found for path: "
              + gvfsPath
              + " for operation MKDIRS. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }
  }

  @Override
  public short getDefaultReplication(Path gvfsPath) {
    try {
      FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
      Path actualFilePath =
          getActualFilePath(
              gvfsPath, currentLocationName(), FilesetDataOperation.GET_DEFAULT_REPLICATION);
      return actualFs.getDefaultReplication(actualFilePath);
    } catch (FileNotFoundException e) {
      return 1;
    }
  }

  @Override
  public long getDefaultBlockSize(Path gvfsPath) {
    try {
      FileSystem actualFs = getActualFileSystem(gvfsPath, currentLocationName());
      Path actualFilePath =
          getActualFilePath(
              gvfsPath, currentLocationName(), FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
      return actualFs.getDefaultBlockSize(actualFilePath);
    } catch (FileNotFoundException e) {
      return defaultBlockSize();
    }
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
    return addDelegationTokensForAllFS(renewer, credentials);
  }
}
