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
package org.apache.gravitino.catalog.hadoop.fs;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public class FileSystemUtils {

  private FileSystemUtils() {}

  public static Map<String, FileSystemProvider> getFileSystemProviders(String fileSystemProviders) {
    Map<String, FileSystemProvider> resultMap = Maps.newHashMap();
    ServiceLoader<FileSystemProvider> allFileSystemProviders =
        ServiceLoader.load(FileSystemProvider.class);

    Set<String> providersInUses =
        fileSystemProviders != null
            ? Arrays.stream(fileSystemProviders.split(","))
                .map(String::trim)
                .collect(java.util.stream.Collectors.toSet())
            : Sets.newHashSet();

    // Only get the file system providers that are in the use list.
    allFileSystemProviders.forEach(
        fileSystemProvider -> {
          if (providersInUses.contains(fileSystemProvider.name())) {
            if (resultMap.containsKey(fileSystemProvider.scheme())) {
              throw new UnsupportedOperationException(
                  String.format(
                      "File system provider with scheme '%s' already exists in the use provider list "
                          + "Please make sure the file system provider scheme is unique.",
                      fileSystemProvider.name()));
            }
            resultMap.put(fileSystemProvider.scheme(), fileSystemProvider);
          }
        });

    // Always add the built-in LocalFileSystemProvider and HDFSFileSystemProvider to the catalog.
    FileSystemProvider builtInLocalFileSystemProvider = new LocalFileSystemProvider();
    FileSystemProvider builtInHDFSFileSystemProvider = new HDFSFileSystemProvider();
    resultMap.put(builtInLocalFileSystemProvider.scheme(), builtInLocalFileSystemProvider);
    resultMap.put(builtInHDFSFileSystemProvider.scheme(), builtInHDFSFileSystemProvider);

    // If not all providersInUses was found, throw an exception.
    Set<String> notFoundProviders =
        Sets.difference(
                providersInUses,
                resultMap.values().stream()
                    .map(FileSystemProvider::name)
                    .collect(Collectors.toSet()))
            .immutableCopy();
    if (!notFoundProviders.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(
              "File system providers %s not found in the classpath. Please make sure the file system "
                  + "provider is in the classpath.",
              notFoundProviders));
    }

    return resultMap;
  }

  public static FileSystemProvider getFileSystemProviderByName(
      Map<String, FileSystemProvider> fileSystemProviders, String fileSystemProviderName) {
    return fileSystemProviders.entrySet().stream()
        .filter(entry -> entry.getValue().name().equals(fileSystemProviderName))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);
  }
}
