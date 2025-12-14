/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.gravitino.catalog.hadoop.fs;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestLocalFileSystemProvider {

  @Test
  public void testGetFileSystem() throws IOException {
    LocalFileSystemProvider provider = new LocalFileSystemProvider();

    Map<String, String> fileSystemProviders =
        ImmutableMap.of(
            "key1", "value1",
            "gravitino.bypass.key2", "value2");

    Path path = new Path("file:///tmp/test");

    FileSystem fs = provider.getFileSystem(path, fileSystemProviders);
    Configuration configuration = fs.getConf();
    // Verify that the configuration contains the expected entries
    Assertions.assertEquals("value1", configuration.get("key1"));
    Assertions.assertEquals("value2", configuration.get("key2"));
    Assertions.assertNull(configuration.get("gravitino.bypass.key2"));
  }
}
