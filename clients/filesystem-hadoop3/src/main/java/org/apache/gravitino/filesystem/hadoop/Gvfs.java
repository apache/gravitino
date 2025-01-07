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
package org.apache.gravitino.filesystem.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * A {@link DelegateToFileSystem} implementation that delegates all operations to a {@link
 * GravitinoVirtualFileSystem}.
 */
public class Gvfs extends DelegateToFileSystem {

  /**
   * Creates a new instance of {@link Gvfs}.
   *
   * @param uri the URI of the file system
   * @param conf the configuration
   * @throws IOException if an I/O error occurs
   * @throws URISyntaxException if the URI is invalid
   */
  protected Gvfs(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(
        uri,
        new GravitinoVirtualFileSystem(),
        conf,
        GravitinoVirtualFileSystemConfiguration.GVFS_SCHEME,
        false);
  }
}
