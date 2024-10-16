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

import static org.apache.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.BUILTIN_HDFS_FS_PROVIDER;
import static org.apache.gravitino.catalog.hadoop.HadoopCatalogPropertiesMetadata.BUILTIN_LOCAL_FS_PROVIDER;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.Locale;
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
                .map(f -> f.trim().toLowerCase(Locale.ROOT))
                .collect(java.util.stream.Collectors.toSet())
            : Sets.newHashSet();

    // Add built-in file system providers to the use list automatically.
    providersInUses.add(BUILTIN_LOCAL_FS_PROVIDER.toLowerCase(Locale.ROOT));
    providersInUses.add(BUILTIN_HDFS_FS_PROVIDER.toLowerCase(Locale.ROOT));

    // Only get the file system providers that are in the user list and check if the scheme is
    // unique.
    Streams.stream(allFileSystemProviders.iterator())
        .filter(
            fileSystemProvider ->
                providersInUses.contains(fileSystemProvider.name().toLowerCase(Locale.ROOT)))
        .forEach(
            fileSystemProvider -> {
              if (resultMap.containsKey(fileSystemProvider.scheme())) {
                throw new UnsupportedOperationException(
                    String.format(
                        "File system provider: '%s' with scheme '%s' already exists in the use provider list "
                            + "Please make sure the file system provider scheme is unique.",
                        fileSystemProvider.getClass().getName(), fileSystemProvider.scheme()));
              }
              resultMap.put(fileSystemProvider.scheme(), fileSystemProvider);
            });

    // If not all file system providers in providersInUses was found, throw an exception.
    Set<String> notFoundProviders =
        Sets.difference(
                providersInUses,
                resultMap.values().stream()
                    .map(p -> p.name().toLowerCase(Locale.ROOT))
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
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format(
                        "File system provider with name '%s' not found in the file system provider list.",
                        fileSystemProviderName)));
  }
}
