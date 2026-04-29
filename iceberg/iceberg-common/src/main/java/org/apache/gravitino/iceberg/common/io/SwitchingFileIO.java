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
package org.apache.gravitino.iceberg.common.io;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.utils.IcebergCatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * A FileIO implementation that dispatches to cloud-specific FileIO implementations by URI scheme.
 */
public class SwitchingFileIO implements FileIO {

  private final Map<String, FileIO> delegateByImpl = new ConcurrentHashMap<>();
  private volatile Map<String, String> properties = Map.of();

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = properties == null ? Map.of() : new HashMap<>(properties);
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate(path).newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return delegate(path).newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate(path).newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate(path).deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return new HashMap<>(properties);
  }

  @Override
  public void close() {
    delegateByImpl
        .values()
        .forEach(
            fileIO -> {
              try {
                fileIO.close();
              } catch (Exception e) {
                throw new RuntimeException("Failed to close delegated FileIO", e);
              }
            });
    delegateByImpl.clear();
  }

  private FileIO delegate(String location) {
    String impl =
        IcebergCatalogUtil.resolveFileIOImplByLocation(location)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Unsupported location for SwitchingFileIO: " + location));
    return delegateByImpl.computeIfAbsent(impl, this::newDelegate);
  }

  private FileIO newDelegate(String implClassName) {
    try {
      Class<?> clazz = Class.forName(implClassName);
      FileIO fileIO = (FileIO) clazz.getDeclaredConstructor().newInstance();
      Map<String, String> delegateProperties = new HashMap<>(properties);
      delegateProperties.put(IcebergConstants.IO_IMPL, implClassName);
      fileIO.initialize(delegateProperties);
      return fileIO;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to initialize FileIO: " + implClassName, e);
    }
  }
}
