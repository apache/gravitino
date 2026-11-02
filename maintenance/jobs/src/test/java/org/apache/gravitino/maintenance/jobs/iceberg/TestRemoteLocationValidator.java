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

/** Tests table-scoped validation on symlink-capable filesystems. */
public class TestRemoteLocationValidator {
  private final StubFileSystem fs = new StubFileSystem();
  private final Path root = new Path("hdfs://host/table");
  private final Path scan = new Path(root, "data");

  /** Verifies object store does not require symlink inspection. */
  @Test
  public void testObjectStoreDoesNotRequireSymlinkInspection() throws Exception {
    fs.symlinksSupported = false;
    RemoteLocationValidator.validate(fs, root, scan);
    assertEquals(0, fs.resolveCalls);
  }

  /** Verifies resolved path outside table is rejected. */
  @Test
  public void testResolvedPathOutsideTableIsRejected() {
    fs.resolvedPaths.put(scan, new Path("hdfs://host/other/data"));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  /** Verifies inspection failure is not ignored. */
  @Test
  public void testInspectionFailureIsNotIgnored() {
    fs.failResolution = true;
    assertThrows(IOException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  /** Verifies symbolic link is rejected. */
  @Test
  public void testSymbolicLinkIsRejected() {
    fs.statuses.put(scan, link(scan));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  /** Verifies descendant symbolic link is rejected. */
  @Test
  public void testDescendantSymbolicLinkIsRejected() {
    fs.listings.put(scan, new FileStatus[] {link(new Path(scan, "link"))});
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
  }

  /** Verifies directories are inspected recursively. */
  @Test
  public void testDirectoriesAreInspectedRecursively() throws Exception {
    Path sub = new Path(scan, "sub");
    fs.listings.put(scan, new FileStatus[] {directory(sub)});
    RemoteLocationValidator.validate(fs, root, scan);
    assertTrue(fs.listedPaths.contains(sub));
  }

  /** Verifies warehouse parent symlink is outside validation scope. */
  @Test
  public void testWarehouseParentSymlinkIsOutsideValidationScope() throws Exception {
    Path table = new Path("hdfs://host/warehouse/table");
    Path data = new Path(table, "data");
    fs.resolvedPaths.put(table, new Path("hdfs://host/storage/table"));
    fs.resolvedPaths.put(data, new Path("hdfs://host/storage/table/data"));
    fs.statuses.put(table.getParent(), link(table.getParent()));
    RemoteLocationValidator.validate(fs, table, data);
    assertTrue(fs.listedPaths.contains(data));
  }

  /** Verifies unreadable parent is outside validation scope. */
  @Test
  public void testUnreadableParentIsOutsideValidationScope() throws Exception {
    fs.unreadablePath = root.getParent();
    RemoteLocationValidator.validate(fs, root, scan);
    assertTrue(fs.listedPaths.contains(scan));
  }

  /** Verifies table root and intermediate symlinks are rejected. */
  @Test
  public void testTableRootAndIntermediateSymlinksAreRejected() {
    fs.statuses.put(root, link(root));
    assertThrows(
        IllegalArgumentException.class, () -> RemoteLocationValidator.validate(fs, root, scan));
    fs.statuses.clear();
    fs.statuses.put(scan, link(scan));
    assertThrows(
        IllegalArgumentException.class,
        () -> RemoteLocationValidator.validate(fs, root, new Path(scan, "nested")));
  }

  /** Verifies normalized paths stop at the table boundary, including a whole-table scan. */
  @Test
  public void testNormalizedPathsAndWholeTableScan() throws Exception {
    fs.unreadablePath = root.getParent();
    RemoteLocationValidator.validate(fs, new Path(root, "."), new Path(root, "staged/../data"));
    assertTrue(fs.listedPaths.contains(scan));
    RemoteLocationValidator.validate(fs, root, root);
    assertTrue(fs.listedPaths.contains(root));
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
    private Path unreadablePath;
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
    public FileStatus getFileLinkStatus(Path path) throws IOException {
      if (path.equals(unreadablePath)) {
        throw new IOException("Permission denied: " + path);
      }
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
