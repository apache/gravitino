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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/** Validates scan paths on Hadoop filesystems that can resolve symbolic links. */
final class RemoteLocationValidator {
  private RemoteLocationValidator() {}

  static void validate(Configuration conf, String tableLocation, String location)
      throws IOException {
    Path scan = new Path(location);
    FileSystem fs = scan.getFileSystem(conf);
    validate(fs, new Path(tableLocation), scan);
  }

  static void validate(FileSystem fs, Path tableLocation, Path scan) throws IOException {
    // Object stores do not support symbolic links. The URI containment check is sufficient there.
    if (!fs.supportsSymlinks()) {
      return;
    }
    Path root = fs.resolvePath(tableLocation);
    IcebergRemoveOrphanFilesJob.validateLocation(root.toString(), fs.resolvePath(scan).toString());
    for (Path ancestor = scan; ancestor != null; ancestor = ancestor.getParent()) {
      if (fs.getFileLinkStatus(ancestor).isSymlink()) {
        throw new IllegalArgumentException("Symlinks are not allowed in the scan location");
      }
    }
    Deque<Path> pending = new ArrayDeque<>();
    pending.add(scan);
    while (!pending.isEmpty()) {
      Path path = pending.removeFirst();
      FileStatus status = fs.getFileLinkStatus(path);
      if (status.isSymlink()) {
        throw new IllegalArgumentException("Symlinks are not allowed in the scan location");
      }
      if (status.isDirectory()) {
        RemoteIterator<FileStatus> children = fs.listStatusIterator(path);
        while (children.hasNext()) {
          FileStatus child = children.next();
          if (child.isSymlink()) {
            throw new IllegalArgumentException("Symlinks are not allowed in the scan location");
          }
          if (child.isDirectory()) {
            pending.addLast(child.getPath());
          }
        }
      }
    }
  }
}
