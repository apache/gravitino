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
package org.apache.gravitino.fileset.gcs;

import java.io.IOException;
import java.net.URI;
import org.apache.gravitino.catalog.hadoop.FileSystemProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GCSFileSystemProvider implements FileSystemProvider {

  @Override
  public FileSystem getFileSystem(Configuration configuration, Path path) throws IOException {
    URI uri = path.toUri();
    if (uri.getScheme() == null || !uri.getScheme().equals("gs")) {
      throw new IllegalArgumentException("The path should be a GCS path.");
    }

    // TODO Check whether GCS related configurations are set such as filesystem.gs.impl, access key,
    //  secret key, etc.

    return FileSystem.get(uri, configuration);
  }

  @Override
  public String getScheme() {
    return "gs";
  }
}
