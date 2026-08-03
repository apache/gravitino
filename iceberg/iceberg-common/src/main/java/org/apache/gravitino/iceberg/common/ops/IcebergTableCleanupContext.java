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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;

/** Immutable information needed to clean up one Iceberg table asynchronously. */
public final class IcebergTableCleanupContext {

  private final String metadataLocation;
  private final String fileIOImpl;
  private final Map<String, String> fileIOProperties;

  /**
   * Creates a cleanup context.
   *
   * @param metadataLocation current Iceberg metadata file location
   * @param fileIOImpl FileIO implementation that can access the table
   * @param fileIOProperties properties used to reconstruct the FileIO
   */
  public IcebergTableCleanupContext(
      String metadataLocation, String fileIOImpl, Map<String, String> fileIOProperties) {
    this.metadataLocation = Objects.requireNonNull(metadataLocation, "metadataLocation");
    this.fileIOImpl = Objects.requireNonNull(fileIOImpl, "fileIOImpl");
    this.fileIOProperties = ImmutableMap.copyOf(fileIOProperties);
  }

  /**
   * @return the snapshotted Iceberg metadata file location
   */
  public String metadataLocation() {
    return metadataLocation;
  }

  /**
   * @return the snapshotted FileIO implementation
   */
  public String fileIOImpl() {
    return fileIOImpl;
  }

  /**
   * @return immutable properties used to reconstruct the FileIO
   */
  public Map<String, String> fileIOProperties() {
    return fileIOProperties;
  }

  @Override
  public String toString() {
    return "IcebergTableCleanupContext{fileIOImpl='"
        + fileIOImpl
        + "', fileIOPropertyCount="
        + fileIOProperties.size()
        + "}";
  }
}
