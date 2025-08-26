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

import static org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystemUtils.getConfigMap;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link GravitinoVirtualFileSystem} is a virtual file system which users can access `fileset` and
 * other resources. It obtains the actual storage location corresponding to the resource from the
 * Apache Gravitino server, and creates an independent file system for it to act as an agent for
 * users to access the underlying storage.
 */
public class GravitinoVirtualFileSystem extends FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoVirtualFileSystem.class);

  private Path workingDirectory;
  private URI uri;
  private GravitinoVirtualFileSystemHook hook;
  private BaseGVFSOperations operations;

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
    }

    String hookClassName =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_HOOK_CLASS_DEFAULT);
    try {
      Class<? extends GravitinoVirtualFileSystemHook> clz =
          (Class<? extends GravitinoVirtualFileSystemHook>) Class.forName(hookClassName);
      this.hook = clz.getDeclaredConstructor().newInstance();
      hook.initialize(getConfigMap(configuration));
    } catch (Exception e) {
      throw new GravitinoRuntimeException(e, "Cannot create hook instance: %s", hookClassName);
    }

    String operationsClassName =
        configuration.get(
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_OPERATIONS_CLASS,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_OPERATIONS_CLASS_DEFAULT);
    try {
      Class<? extends BaseGVFSOperations> operationsClz =
          (Class<? extends BaseGVFSOperations>) Class.forName(operationsClassName);
      this.operations =
          operationsClz.getDeclaredConstructor(Configuration.class).newInstance(configuration);
    } catch (Exception e) {
      if (e instanceof InvocationTargetException
          && ((InvocationTargetException) e).getTargetException() instanceof RuntimeException) {
        throw (RuntimeException) ((InvocationTargetException) e).getTargetException();
      }
      throw new GravitinoRuntimeException(
          e, "Cannot create operations instance: %s", operationsClassName);
    }

    this.workingDirectory = new Path(name);
    this.uri = URI.create(name.getScheme() + "://" + name.getAuthority());

    setConf(configuration);
    super.initialize(uri, getConf());
  }

  @VisibleForTesting
  GravitinoVirtualFileSystemHook getHook() {
    return hook;
  }

  @VisibleForTesting
  BaseGVFSOperations getOperations() {
    return operations;
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public synchronized Path getWorkingDirectory() {
    return this.workingDirectory;
  }

  @Override
  public synchronized void setWorkingDirectory(Path newDir) {
    Path newPath = hook.preSetWorkingDirectory(newDir);
    try {
      runWithExceptionTranslation(
          () -> {
            operations.setWorkingDirectory(newPath);
            return null;
          },
          FilesetDataOperation.SET_WORKING_DIR);
    } catch (FilesetPathNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.workingDirectory = newPath;
    hook.postSetWorkingDirectory(newPath);
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    Path newPath = hook.preOpen(path, bufferSize);
    return hook.postOpen(
        newPath,
        bufferSize,
        runWithExceptionTranslation(
            () -> operations.open(newPath, bufferSize), FilesetDataOperation.OPEN));
  }

  @Override
  public FSDataOutputStream create(
      Path path,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    Path newPath = hook.preCreate(path, permission, overwrite, bufferSize, replication, blockSize);
    try {
      return hook.postCreate(
          newPath,
          permission,
          overwrite,
          bufferSize,
          replication,
          blockSize,
          operations.create(
              newPath, permission, overwrite, bufferSize, replication, blockSize, progress));
    } catch (NoSuchCatalogException
        | CatalogNotInUseException
        | NoSuchFilesetException
        | NoSuchLocationNameException
        | FilesetPathNotFoundException e) {
      String message =
          "Fileset is not found for path: "
              + path
              + " for operation CREATE. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress)
      throws IOException {
    Path newPath = hook.preAppend(path, bufferSize);
    return hook.postAppend(
        newPath,
        bufferSize,
        runWithExceptionTranslation(
            () -> operations.append(newPath, bufferSize, progress), FilesetDataOperation.APPEND));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    Pair<Path, Path> pair = hook.preRename(src, dst);
    return hook.postRename(
        pair.getLeft(),
        pair.getRight(),
        runWithExceptionTranslation(
            () -> operations.rename(pair.getLeft(), pair.getRight()), FilesetDataOperation.RENAME));
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    Path newPath = hook.preDelete(path, recursive);
    try {
      return hook.postDelete(
          newPath,
          recursive,
          runWithExceptionTranslation(
              () -> operations.delete(newPath, recursive), FilesetDataOperation.DELETE));
    } catch (FilesetPathNotFoundException e) {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    Path newPath = hook.preGetFileStatus(path);
    return hook.postGetFileStatus(
        runWithExceptionTranslation(
            () -> operations.getFileStatus(newPath), FilesetDataOperation.GET_FILE_STATUS));
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    Path newPath = hook.preListStatus(path);
    return hook.postListStatus(
        runWithExceptionTranslation(
            () -> operations.listStatus(newPath), FilesetDataOperation.LIST_STATUS));
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    Path newPath = hook.preMkdirs(path, permission);
    try {
      return hook.postMkdirs(newPath, permission, operations.mkdirs(newPath, permission));
    } catch (NoSuchCatalogException
        | CatalogNotInUseException
        | NoSuchFilesetException
        | NoSuchLocationNameException
        | FilesetPathNotFoundException e) {
      String message =
          "Fileset is not found for path: "
              + newPath
              + " for operation MKDIRS. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }
  }

  @Override
  public short getDefaultReplication(Path f) {
    Path newPath = hook.preGetDefaultReplication(f);
    try {
      return hook.postGetDefaultReplication(
          newPath,
          runWithExceptionTranslation(
              () -> operations.getDefaultReplication(newPath),
              FilesetDataOperation.GET_DEFAULT_REPLICATION));
    } catch (IOException e) {
      return 1;
    }
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    Path newPath = hook.preGetDefaultBlockSize(f);
    try {
      return hook.postGetDefaultBlockSize(
          newPath,
          runWithExceptionTranslation(
              () -> operations.getDefaultBlockSize(newPath),
              FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE));
    } catch (IOException e) {
      return operations.defaultBlockSize();
    }
  }

  @Override
  public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) {
    return operations.addDelegationTokens(renewer, credentials);
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      hook.close();
    } catch (IOException e) {
      LOG.warn("Failed to close hook: {}", hook.getClass().getName(), e);
    }

    try {
      operations.close();
    } catch (IOException e) {
      LOG.warn("Failed to close operations: {}", operations.getClass().getName(), e);
    }

    super.close();
  }

  private <R, E extends IOException> R runWithExceptionTranslation(
      Executable<R, E> executable, FilesetDataOperation operation)
      throws FilesetPathNotFoundException, E {
    try {
      return executable.execute();
    } catch (NoSuchCatalogException | CatalogNotInUseException e) {
      String message = String.format("Cannot get fileset catalog during %s", operation);
      LOG.warn(message, e);
      throw new FilesetPathNotFoundException(message, e);

    } catch (NoSuchFilesetException e) {
      String message = String.format("Cannot get fileset during %s", operation);
      LOG.warn(message, e);
      throw new FilesetPathNotFoundException(message, e);

    } catch (NoSuchLocationNameException e) {
      String message = String.format("Cannot find location name during %s", operation);
      LOG.warn(message, e);
      throw new FilesetPathNotFoundException(message, e);

    } catch (IOException e) {
      throw e;
    }
  }

  @FunctionalInterface
  private interface Executable<R, E extends Exception> {
    R execute() throws E;
  }
}
