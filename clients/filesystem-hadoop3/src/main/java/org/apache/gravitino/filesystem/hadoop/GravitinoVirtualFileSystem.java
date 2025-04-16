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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
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
  private BaseGVFSOperations operations;

  @Override
  public void initialize(URI name, Configuration configuration) throws IOException {
    if (!name.toString().startsWith(GravitinoVirtualFileSystemConfiguration.GVFS_FILESET_PREFIX)) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported file system scheme: %s for %s.",
              name.getScheme(), GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME));
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
    try {
      runWithExceptionTranslation(
          () -> {
            operations.setWorkingDirectory(newDir);
            return null;
          },
          FilesetDataOperation.SET_WORKING_DIR);
    } catch (FilesetPathNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.workingDirectory = newDir;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return runWithExceptionTranslation(
        () -> operations.open(path, bufferSize), FilesetDataOperation.OPEN);
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
    try {
      return operations.create(
          path, permission, overwrite, bufferSize, replication, blockSize, progress);
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
    return runWithExceptionTranslation(
        () -> operations.append(path, bufferSize, progress), FilesetDataOperation.APPEND);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return runWithExceptionTranslation(
        () -> operations.rename(src, dst), FilesetDataOperation.RENAME);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      return runWithExceptionTranslation(
          () -> operations.delete(path, recursive), FilesetDataOperation.DELETE);
    } catch (FilesetPathNotFoundException e) {
      return false;
    }
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return runWithExceptionTranslation(
        () -> operations.getFileStatus(path), FilesetDataOperation.GET_FILE_STATUS);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    return runWithExceptionTranslation(
        () -> operations.listStatus(path), FilesetDataOperation.LIST_STATUS);
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    try {
      return operations.mkdirs(path, permission);
    } catch (NoSuchCatalogException
        | CatalogNotInUseException
        | NoSuchFilesetException
        | NoSuchLocationNameException
        | FilesetPathNotFoundException e) {
      String message =
          "Fileset is not found for path: "
              + path
              + " for operation MKDIRS. "
              + "This may be caused by fileset related metadata not found or not in use in "
              + "Gravitino, please check the fileset metadata in Gravitino.";
      throw new IOException(message, e);
    }
  }

  @Override
  public short getDefaultReplication(Path f) {
    try {
      return runWithExceptionTranslation(
          () -> operations.getDefaultReplication(f), FilesetDataOperation.GET_DEFAULT_REPLICATION);
    } catch (IOException e) {
      return 1;
    }
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    try {
      return runWithExceptionTranslation(
          () -> operations.getDefaultBlockSize(f), FilesetDataOperation.GET_DEFAULT_BLOCK_SIZE);
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
      LOG.warn(message, operation, e);
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
      if (e instanceof FilesetPathNotFoundException) {
        throw (FilesetPathNotFoundException) e;
      }
      throw e;
    }
  }

  @FunctionalInterface
  private interface Executable<R, E extends Exception> {
    R execute() throws E;
  }
}
