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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;

class TestRemoteLocationValidator {
  private final FileSystem fs = mock(FileSystem.class);
  private final Path root = new Path("hdfs://host/table");
  private final Path scan = new Path(root, "data");

  @Test
  void testObjectStoreDoesNotRequireSymlinkInspection() throws Exception {
    when(fs.supportsSymlinks()).thenReturn(false);
    RemoteLocationValidator.validate(fs, root, scan);
    verify(fs, never()).resolvePath(any());
  }

  @Test
  void testResolvedPathOutsideTableIsRejected() throws Exception {
    when(fs.supportsSymlinks()).thenReturn(true);
    when(fs.resolvePath(root)).thenReturn(root);
    when(fs.resolvePath(scan)).thenReturn(new Path("hdfs://host/other/data"));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testInspectionFailureIsNotIgnored() throws Exception {
    when(fs.supportsSymlinks()).thenReturn(true);
    when(fs.resolvePath(root)).thenThrow(new IOException("Permission denied"));
    assertThrows(IOException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testSymbolicLinkIsRejected() throws Exception {
    prepare();
    when(fs.getFileLinkStatus(scan)).thenReturn(link(scan));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testDescendantSymbolicLinkIsRejected() throws Exception {
    prepare();
    when(fs.listStatusIterator(scan)).thenReturn(children(link(new Path(scan, "link"))));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testDirectoriesAreInspectedRecursively() throws Exception {
    prepare();
    Path sub = new Path(scan, "sub");
    when(fs.listStatusIterator(scan)).thenReturn(children(directory(sub)));
    when(fs.listStatusIterator(sub)).thenReturn(children());
    RemoteLocationValidator.validate(fs, root, scan);
    verify(fs).listStatusIterator(sub);
  }

  private void prepare() throws Exception {
    when(fs.supportsSymlinks()).thenReturn(true);
    when(fs.resolvePath(root)).thenReturn(root);
    when(fs.resolvePath(scan)).thenReturn(scan);
    when(fs.getFileLinkStatus(any()))
        .thenAnswer(invocation -> directory(invocation.getArgument(0)));
  }

  private static FileStatus directory(Path path) {
    return new FileStatus(0, true, 1, 0, 0, path);
  }

  private static FileStatus link(Path path) {
    FileStatus status = directory(path);
    status.setSymlink(new Path("hdfs://host/other"));
    return status;
  }

  private static RemoteIterator<FileStatus> children(FileStatus... statuses) {
    return new RemoteIterator<FileStatus>() {
      private int index;

      @Override
      public boolean hasNext() {
        return index < statuses.length;
      }

      @Override
      public FileStatus next() {
        return statuses[index++];
      }
    };
  }
}
