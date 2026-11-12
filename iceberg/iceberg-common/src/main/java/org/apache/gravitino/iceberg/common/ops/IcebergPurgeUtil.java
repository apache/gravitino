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
package org.apache.gravitino.iceberg.common.ops;

import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Deletes a dropped table's files when some of them can no longer be read. */
final class IcebergPurgeUtil {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergPurgeUtil.class);

  private IcebergPurgeUtil() {}

  /**
   * Deletes every data, delete, manifest, manifest list, statistics and metadata file reachable
   * from the metadata, skipping the parts that cannot be read. A missing manifest list only loses
   * the files listed in it. Content files are preserved when {@code gc.enabled} is false.
   *
   * @param io the table's FileIO
   * @param metadata the table metadata read before the table was dropped
   */
  static void deleteReachableFiles(FileIO io, TableMetadata metadata) {
    boolean gcEnabled =
        PropertyUtil.propertyAsBoolean(
            metadata.properties(), TableProperties.GC_ENABLED, TableProperties.GC_ENABLED_DEFAULT);
    Set<String> manifestLists = new HashSet<>();
    Set<String> manifests = new HashSet<>();
    Set<String> contentFiles = new HashSet<>();
    for (Snapshot snapshot : metadata.snapshots()) {
      if (snapshot.manifestListLocation() != null) {
        manifestLists.add(snapshot.manifestListLocation());
      }
      try {
        for (ManifestFile manifest : snapshot.allManifests(io)) {
          if (manifests.add(manifest.path()) && gcEnabled) {
            collectContentFiles(io, metadata, manifest, contentFiles);
          }
        }
      } catch (RuntimeException e) {
        LOG.warn(
            "Cannot read the manifest list of snapshot {}; its files are not deleted: {}",
            snapshot.snapshotId(),
            e.getMessage());
      }
    }
    Set<String> metadataFiles = new HashSet<>();
    metadata.previousFiles().forEach(entry -> metadataFiles.add(entry.file()));
    if (metadata.metadataFileLocation() != null) {
      metadataFiles.add(metadata.metadataFileLocation());
    }
    for (StatisticsFile stats : metadata.statisticsFiles()) {
      metadataFiles.add(stats.path());
    }
    metadata.partitionStatisticsFiles().forEach(stats -> metadataFiles.add(stats.path()));

    deleteAll(io, contentFiles);
    deleteAll(io, manifests);
    deleteAll(io, manifestLists);
    deleteAll(io, metadataFiles);
  }

  private static void collectContentFiles(
      FileIO io, TableMetadata metadata, ManifestFile manifest, Set<String> paths) {
    try {
      if (manifest.content() == ManifestContent.DATA) {
        try (ManifestReader<DataFile> reader =
            ManifestFiles.read(manifest, io, metadata.specsById())) {
          reader.forEach(file -> paths.add(file.location()));
        }
      } else {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, io, metadata.specsById())) {
          reader.forEach(file -> paths.add(file.location()));
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "Cannot read manifest {}; its files are not deleted: {}",
          manifest.path(),
          e.getMessage());
    }
  }

  private static void deleteAll(FileIO io, Set<String> paths) {
    for (String path : paths) {
      try {
        io.deleteFile(path);
      } catch (RuntimeException e) {
        LOG.warn("Cannot delete {}: {}", path, e.getMessage());
      }
    }
  }
}
