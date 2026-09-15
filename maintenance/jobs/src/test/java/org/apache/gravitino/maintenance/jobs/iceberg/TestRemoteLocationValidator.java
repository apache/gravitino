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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Test;

class TestRemoteLocationValidator {
  private final StubFileSystem fs = new StubFileSystem();
  private final Path root = new Path("hdfs://host/table");
  private final Path scan = new Path(root, "data");

  @Test
  void testObjectStoreDoesNotRequireSymlinkInspection() throws Exception {
    fs.symlinksSupported = false;
    RemoteLocationValidator.validate(fs, root, scan);
    assertEquals(0, fs.resolveCalls);
  }

  @Test
  void testResolvedPathOutsideTableIsRejected() {
    fs.resolvedPaths.put(scan, new Path("hdfs://host/other/data"));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testInspectionFailureIsNotIgnored() {
    fs.failResolution = true;
    assertThrows(IOException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testSymbolicLinkIsRejected() {
    fs.statuses.put(scan, link(scan));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testDescendantSymbolicLinkIsRejected() {
    fs.listings.put(scan, new FileStatus[] {link(new Path(scan, "link"))});
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  @Test
  void testDirectoriesAreInspectedRecursively() throws Exception {
    Path sub = new Path(scan, "sub");
    fs.listings.put(scan, new FileStatus[] {directory(sub)});
    RemoteLocationValidator.validate(fs, root, scan);
    assertTrue(fs.listedPaths.contains(sub));
  }

  private static FileStatus directory(Path path) {
    return new FileStatus(0, true, 1, 0, 0, path);
  }

  private static FileStatus link(Path path) {
    FileStatus status = directory(path);
    status.setSymlink(new Path("hdfs://host/other"));
    return status;
  }

  /**
   * In-memory filesystem responses for deterministic validation tests without external services.
   */
  private static class StubFileSystem extends FilterFileSystem {
    private final Map<Path, Path> resolvedPaths = new HashMap<>();
    private final Map<Path, FileStatus> statuses = new HashMap<>();
    private final Map<Path, FileStatus[]> listings = new HashMap<>();
    private final List<Path> listedPaths = new ArrayList<>();
    private boolean symlinksSupported = true;
    private boolean failResolution;
    private int resolveCalls;

    @Override
    public boolean supportsSymlinks() {
      return symlinksSupported;
    }

    @Override
    public Path resolvePath(Path path) throws IOException {
      resolveCalls++;
      if (failResolution) {
        throw new IOException("Permission denied");
      }
      return resolvedPaths.getOrDefault(path, path);
    }

    @Override
    public FileStatus getFileLinkStatus(Path path) {
      return statuses.getOrDefault(path, directory(path));
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path path) {
      listedPaths.add(path);
      FileStatus[] children = listings.getOrDefault(path, new FileStatus[0]);
      return new RemoteIterator<FileStatus>() {
        private int index;

        @Override
        public boolean hasNext() {
          return index < children.length;
        }

        @Override
        public FileStatus next() {
          return children[index++];
        }
      };
    }
  }
}
