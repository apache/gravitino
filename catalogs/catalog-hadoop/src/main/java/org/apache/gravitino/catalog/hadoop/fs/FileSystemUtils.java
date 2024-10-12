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

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;

public class FileSystemUtils {

  private FileSystemUtils() {}

  public static void initFileSystemProviders(
      String fileSystemProviders, Map<String, FileSystemProvider> fileProviders) {
    if (StringUtils.isBlank(fileSystemProviders)) {
      return;
    }

    String[] providers = fileSystemProviders.split(",");
    for (String provider : providers) {
      try {
        FileSystemProvider fileSystemProvider =
            (FileSystemProvider)
                Class.forName(provider.trim()).getDeclaredConstructor().newInstance();
        fileProviders.put(fileSystemProvider.getScheme(), fileSystemProvider);
      } catch (Exception e) {
        throw new GravitinoRuntimeException(
            e, "Failed to initialize file system provider: %s", provider);
      }
    }
  }
}
