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

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

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
  public void close() throws IOException {}
}
