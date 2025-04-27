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

import java.io.Closeable;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * A hook interface for injecting custom logic before the Gravitino Virtual File System operations.
 * The implementor should handle the exception, if any, in the pre-hook method, otherwise the
 * exception will be thrown to the caller and fail the operation. Besides, the implemented pre-hook
 * method should be lightweight and fast, otherwise it will slow down the operation. The pre-hook
 * method may be called more than once and in parallel, so the implementor should handle the
 * concurrent and idempotent issues if required.
 */
public interface GravitinoVirtualFileSystemHook extends Closeable {

  /**
   * Initialize the hook with the configuration. This method will be called in the GVFS initialize
   * method, and the configuration will be passed from the GVFS configuration. The implementor can
   * initialize the hook with the configuration. The exception will be thrown to the caller and fail
   * the GVFS initialization.
   *
   * @param config The configuration.
   */
  void initialize(Map<String, String> config);

  /**
   * Pre-hook for setWorkingDirectory operation. This method will be called before the
   * setWorkingDirectory operation. The returned path will be used for the setWorkingDirectory
   * operation. The implementor can modify the path for customization. The exception will be thrown
   * to the caller and fail the setWorkingDirectory operation.
   *
   * @param path The path to set working directory.
   * @return The path to set working directory.
   */
  Path preSetWorkingDirectory(Path path);

  /**
   * Post-hook for setWorkingDirectory operation. This method will be called after the
   * setWorkingDirectory operation. The fileset path will be passed to the post-hook method. The
   * implementor can do some post-processing after the setWorkingDirectory operation. The exception
   * will be thrown to the caller and fail the setWorkingDirectory operation.
   *
   * @param gvfsPath the GVFS path to be set as the working directory
   */
  void postSetWorkingDirectory(Path gvfsPath);

  /**
   * Pre-hook for open operation. This method will be called before the open operation. The returned
   * path will be used for the open operation. The implementor can modify the path for
   * customization. The exception will be thrown to the caller and fail the open operation.
   *
   * @param path The path to open.
   * @param bufferSize The buffer size.
   * @return The path to open.
   */
  Path preOpen(Path path, int bufferSize);

  /**
   * Post-hook for open operation. This method will be called after the open operation. The input
   * stream will be passed to the post-hook method. The implementor can do some post-processing
   * after the open operation. The exception will be thrown to the caller and fail the open
   * operation.
   *
   * @param gvfsPath The GVFS path to open.
   * @param bufferSize The buffer size to open the file.
   * @param inputStream The input stream.
   * @return The input stream.
   */
  FSDataInputStream postOpen(Path gvfsPath, int bufferSize, FSDataInputStream inputStream);

  /**
   * Pre-hook for create operation. This method will be called before the create operation. The
   * returned path will be used for the create operation. The implementor can modify the path for
   * customization. The exception will be thrown to the caller and fail the create operation.
   *
   * @param path The path to create.
   * @param permission The permission.
   * @param overwrite Whether to overwrite the file.
   * @param bufferSize The buffer size.
   * @param replication The replication factor.
   * @param blockSize The block size.
   * @return The path to create.
   */
  Path preCreate(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize);

  /**
   * Post-hook for create operation. This method will be called after the create operation. The
   * output stream will be passed to the post-hook method. The implementor can do some
   * post-processing after the create operation. The exception will be thrown to the caller and fail
   * the create operation.
   *
   * @param gvfsPath The GVFS path to create.
   * @param permission The permission.
   * @param overwrite Whether to overwrite the file.
   * @param bufferSize The buffer size.
   * @param replication The replication factor.
   * @param blockSize The block size.
   * @param outputStream The output stream.
   * @return The output stream.
   */
  FSDataOutputStream postCreate(
      Path gvfsPath,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      FSDataOutputStream outputStream);

  /**
   * Pre-hook for append operation. This method will be called before the append operation. The
   * returned path will be used for the append operation. The implementor can modify the path for
   * customization. The exception will be thrown to the caller and fail the append operation.
   *
   * @param path The path to append.
   * @param bufferSize The buffer size.
   * @return The path to append.
   */
  Path preAppend(Path path, int bufferSize);

  /**
   * Post-hook for append operation. This method will be called after the append operation. The
   * output stream will be passed to the post-hook method. The implementor can do some
   * post-processing after the append operation. The exception will be thrown to the caller and fail
   * the append operation.
   *
   * @param gvfsPath The GVFS path to append.
   * @param bufferSize The buffer size.
   * @param outputStream The output stream.
   * @return The output stream.
   */
  FSDataOutputStream postAppend(Path gvfsPath, int bufferSize, FSDataOutputStream outputStream);

  /**
   * Pre-hook for rename operation. This method will be called before the rename operation. The
   * source path and destination path will be passed to the pre-hook method. The implementor can can
   * modify the source path and destination path for customization. The exception will be thrown to
   * the caller and fail the rename operation.
   *
   * @param src The source path.
   * @param dst The destination path.
   * @return The pair of source path and destination path.
   */
  Pair<Path, Path> preRename(Path src, Path dst);

  /**
   * Post-hook for rename operation. This method will be called after the rename operation. The
   * implementor can do some post-processing after the rename operation. The exception will be
   * thrown to the caller and fail the rename operation.
   *
   * @param srcGVFSPath The source GVFS path.
   * @param dstGVFSPath The destination GVFS path.
   * @param success Whether the rename operation is successful.
   * @return Whether the rename operation is successful.
   */
  boolean postRename(Path srcGVFSPath, Path dstGVFSPath, boolean success);

  /**
   * Pre-hook for delete operation. This method will be called before the delete operation. The
   * returned path will be used for the delete operation. The implementor can modify the path for
   * customization. The exception will be thrown to the caller and fail the delete operation.
   *
   * @param path The path to delete.
   * @param recursive Whether to delete recursively.
   * @return The path to delete.
   */
  Path preDelete(Path path, boolean recursive);

  /**
   * Post-hook for delete operation. This method will be called after the delete operation. The
   * implementor can do some post-processing after the delete operation. The exception will be
   * thrown to the caller and fail the delete operation.
   *
   * @param gvfsPath The GVFS path to delete.
   * @param recursive Whether to delete recursively.
   * @param success Whether the delete operation is successful.
   * @return Whether the delete operation is successful.
   */
  boolean postDelete(Path gvfsPath, boolean recursive, boolean success);

  /**
   * Pre-hook for getFileStatus operation. This method will be called before the getFileStatus. The
   * returned path will be used for the getFileStatus operation. The implementor can modify the path
   * for customization. The exception will be thrown to the caller and fail the getFileStatus
   * operation.
   *
   * @param path The path to get file status.
   * @return The path to get file status.
   */
  Path preGetFileStatus(Path path);

  /**
   * Post-hook for getFileStatus operation. This method will be called after the getFileStatus
   * operation. The file status will be passed to the post-hook method. The implementor can do some
   * post-processing after the getFileStatus operation. The exception will be thrown to the caller
   * and fail the getFileStatus operation.
   *
   * @param fileStatus The file status.
   * @return The file status.
   */
  FileStatus postGetFileStatus(FileStatus fileStatus);

  /**
   * Pre-hook for listStatus operation.
   *
   * @param path The path to list status.
   * @return The path to list status.
   */
  Path preListStatus(Path path);

  /**
   * Post-hook for listStatus operation. This method will be called after the listStatus operation.
   * The file statuses will be passed to the post-hook method. The implementor can do some
   * post-processing after the listStatus operation. The exception will be thrown to the caller and
   * fail the listStatus operation.
   *
   * @param fileStatuses The file statuses.
   * @return The file statuses.
   */
  FileStatus[] postListStatus(FileStatus[] fileStatuses);

  /**
   * Pre-hook for mkdirs operation. This method will be called before the mkdirs operation. The
   * returned path will be used for the mkdirs operation. The implementor can modify the path for
   * customization. The exception will be thrown to the caller and fail the mkdirs operation.
   *
   * @param path The path to mkdirs.
   * @param permission The permission.
   * @return The path to mkdirs.
   */
  Path preMkdirs(Path path, FsPermission permission);

  /**
   * Post-hook for mkdirs operation. This method will be called after the mkdirs operation. The
   * implementor can do some post-processing after the mkdirs operation. The exception will be
   * thrown to the caller and fail the mkdirs operation.
   *
   * @param gvfsPath The GVFS path to mkdirs.
   * @param permission The permission.
   * @param success Whether the mkdirs operation is successful.
   * @return Whether the mkdirs operation is successful.
   */
  boolean postMkdirs(Path gvfsPath, FsPermission permission, boolean success);

  /**
   * Pre-hook for getDefaultReplication operation. This method will be called before the
   * getDefaultReplication operation. The returned path will be used for the getDefaultReplication
   * operation. The implementor can modify the path for customization. The exception will be thrown
   * to the caller and fail the getDefaultReplication operation.
   *
   * @param path The path to get default replication.
   * @return The path to get default replication.
   */
  Path preGetDefaultReplication(Path path);

  /**
   * Post-hook for getDefaultReplication operation. This method will be called after the
   * getDefaultReplication operation. The default replication will be passed to the post-hook
   * method. The implementor can do some post-processing after the getDefaultReplication operation.
   * The exception will be thrown to the caller and fail the getDefaultReplication operation.
   *
   * @param gvfsPath The GVFS path to get default replication.
   * @param replication The default replication.
   * @return The default replication.
   */
  short postGetDefaultReplication(Path gvfsPath, short replication);

  /**
   * Pre-hook for getDefaultBlockSize operation. This method will be called before the
   * getDefaultBlockSize operation. The returned path will be used for the getDefaultBlockSize
   * operation. The implementor can modify the path for customization. The exception will be thrown
   * to the caller and fail the getDefaultBlockSize operation.
   *
   * @param path The path to get default block size.
   * @return The path to get default block size.
   */
  Path preGetDefaultBlockSize(Path path);

  /**
   * Post-hook for getDefaultBlockSize operation. This method will be called after the
   * getDefaultBlockSize operation. The default block size will be passed to the post-hook method.
   * The implementor can do some post-processing after the getDefaultBlockSize operation. The
   * exception will be thrown to the caller and fail the getDefaultBlockSize operation.
   *
   * @param gvfsPath The GVFS path to get default block size.
   * @param blockSize The default block size.
   * @return The default block size.
   */
  long postGetDefaultBlockSize(Path gvfsPath, long blockSize);
}
