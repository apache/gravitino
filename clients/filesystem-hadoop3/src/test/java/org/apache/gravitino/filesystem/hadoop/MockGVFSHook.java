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

public class MockGVFSHook implements GravitinoVirtualFileSystemHook {

  boolean preSetWorkingDirectoryCalled = false;
  boolean preOpenCalled = false;
  boolean preCreateCalled = false;
  boolean preAppendCalled = false;
  boolean preRenameCalled = false;
  boolean preDeleteCalled = false;
  boolean preGetFileStatusCalled = false;
  boolean preListStatusCalled = false;
  boolean preMkdirsCalled = false;
  boolean preGetDefaultReplicationCalled = false;
  boolean preGetDefaultBlockSizeCalled = false;
  boolean postSetWorkingDirectoryCalled = false;
  boolean postOpenCalled = false;
  boolean postCreateCalled = false;
  boolean postAppendCalled = false;
  boolean postRenameCalled = false;
  boolean postDeleteCalled = false;
  boolean postGetFileStatusCalled = false;
  boolean postListStatusCalled = false;
  boolean postMkdirsCalled = false;
  boolean postGetDefaultReplicationCalled = false;
  boolean postGetDefaultBlockSizeCalled = false;

  @Override
  public void initialize(Map<String, String> config) {}

  @Override
  public Path preSetWorkingDirectory(Path path) {
    this.preSetWorkingDirectoryCalled = true;
    return path;
  }

  @Override
  public void postSetWorkingDirectory(Path gvfsPath) {
    this.postSetWorkingDirectoryCalled = true;
  }

  @Override
  public Path preOpen(Path path, int bufferSize) {
    this.preOpenCalled = true;
    return path;
  }

  @Override
  public FSDataInputStream postOpen(Path gvfsPath, int bufferSize, FSDataInputStream inputStream) {
    this.postOpenCalled = true;
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
    this.preCreateCalled = true;
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
    this.postCreateCalled = true;
    return outputStream;
  }

  @Override
  public Path preAppend(Path path, int bufferSize) {
    this.preAppendCalled = true;
    return path;
  }

  @Override
  public FSDataOutputStream postAppend(
      Path gvfsPath, int bufferSize, FSDataOutputStream outputStream) {
    this.postAppendCalled = true;
    return outputStream;
  }

  @Override
  public Pair<Path, Path> preRename(Path src, Path dst) {
    this.preRenameCalled = true;
    return Pair.of(src, dst);
  }

  @Override
  public boolean postRename(Path srcGvfsPath, Path dstGvfsPath, boolean success) {
    this.postRenameCalled = true;
    return success;
  }

  @Override
  public Path preDelete(Path path, boolean recursive) {
    this.preDeleteCalled = true;
    return path;
  }

  @Override
  public boolean postDelete(Path gvfsPath, boolean recursive, boolean success) {
    this.postDeleteCalled = true;
    return success;
  }

  @Override
  public Path preGetFileStatus(Path path) {
    this.preGetFileStatusCalled = true;
    return path;
  }

  @Override
  public FileStatus postGetFileStatus(FileStatus fileStatus) {
    this.postGetFileStatusCalled = true;
    return fileStatus;
  }

  @Override
  public Path preListStatus(Path path) {
    this.preListStatusCalled = true;
    return path;
  }

  @Override
  public FileStatus[] postListStatus(FileStatus[] fileStatuses) {
    this.postListStatusCalled = true;
    return fileStatuses;
  }

  @Override
  public Path preMkdirs(Path path, FsPermission permission) {
    this.preMkdirsCalled = true;
    return path;
  }

  @Override
  public boolean postMkdirs(Path gvfsPath, FsPermission permission, boolean success) {
    this.postMkdirsCalled = true;
    return success;
  }

  @Override
  public Path preGetDefaultReplication(Path path) {
    this.preGetDefaultReplicationCalled = true;
    return path;
  }

  @Override
  public short postGetDefaultReplication(Path gvfsPath, short replication) {
    this.postGetDefaultReplicationCalled = true;
    return replication;
  }

  @Override
  public Path preGetDefaultBlockSize(Path path) {
    this.preGetDefaultBlockSizeCalled = true;
    return path;
  }

  @Override
  public long postGetDefaultBlockSize(Path gvfsPath, long blockSize) {
    this.postGetDefaultBlockSizeCalled = true;
    return blockSize;
  }

  @Override
  public void close() throws IOException {}
}
