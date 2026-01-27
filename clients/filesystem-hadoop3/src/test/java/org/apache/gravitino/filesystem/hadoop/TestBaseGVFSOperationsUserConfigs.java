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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.utils.FilesetUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** Unit test covering the user-defined configuration extraction logic in BaseGVFSOperations. */
public class TestBaseGVFSOperationsUserConfigs {

  private Configuration createBaseConfiguration() {
    Configuration conf = new Configuration();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, "test_metalake");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY,
        "http://localhost:8090");
    return conf;
  }

  @Test
  public void testGetUserDefinedConfigsReturnsExpectedEntries() throws Exception {
    Configuration conf = createBaseConfiguration();
    // Define location1 with base location hdfs://cluster1
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX + "cluster1",
        "hdfs://cluster1");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX + "cluster1.aws-ak",
        "AK1");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX + "cluster1.aws-sk",
        "SK1");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX
            + "cluster1.config.resource",
        "/etc/core-site.xml,hdfs-site.xml");
    // Another location's property which should be ignored
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX + "cluster2",
        "hdfs://cluster2");
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX + "cluster2.aws-ak",
        "AK2");

    // Convert Configuration to Map for FilesetUtil
    Map<String, String> confMap = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      confMap.put(entry.getKey(), entry.getValue());
    }

    // Test with path that matches cluster1 (same baseLocation: hdfs://cluster1)
    URI testPath = new URI("hdfs://cluster1/path/to/file");
    Map<String, String> properties =
        FilesetUtil.getUserDefinedFileSystemConfigs(
            testPath,
            confMap,
            GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_PATH_CONFIG_PREFIX);

    assertEquals(3, properties.size(), "Only cluster1 scoped properties should be returned");
    assertEquals("AK1", properties.get("aws-ak"));
    assertEquals("SK1", properties.get("aws-sk"));
    assertEquals("/etc/core-site.xml,hdfs-site.xml", properties.get("config.resource"));
  }
}
